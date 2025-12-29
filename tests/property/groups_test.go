package property

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/google/uuid"
)

// GroupOp represents a group coordinator operation for property testing.
type GroupOp struct {
	OpType int // 0=join, 1=leave, 2=heartbeat
}

// Generate implements quick.Generator for GroupOp.
func (GroupOp) Generate(rand *rand.Rand, size int) reflect.Value {
	op := GroupOp{
		OpType: rand.Intn(3),
	}
	return reflect.ValueOf(op)
}

// MemberOp represents a specific operation on a member.
type MemberOp struct {
	OpType   int    // 0=join, 1=leave, 2=heartbeat
	MemberID string // Which member to operate on
}

// TestPropertyStateTransitionsValid verifies that state transitions follow
// the valid state machine rules.
func TestPropertyStateTransitionsValid(t *testing.T) {
	// Define valid state transitions per Kafka group coordinator spec
	validTransitions := map[groups.GroupStateName][]groups.GroupStateName{
		groups.GroupStateEmpty:              {groups.GroupStatePreparingRebalance, groups.GroupStateDead},
		groups.GroupStateStable:             {groups.GroupStatePreparingRebalance, groups.GroupStateDead},
		groups.GroupStatePreparingRebalance: {groups.GroupStateCompletingRebalance, groups.GroupStateEmpty, groups.GroupStateDead},
		groups.GroupStateCompletingRebalance: {groups.GroupStateStable, groups.GroupStatePreparingRebalance, groups.GroupStateDead},
		groups.GroupStateDead:               {}, // Dead is terminal
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		state, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Logf("CreateGroup failed: %v", err)
			return false
		}

		if state.State != groups.GroupStateEmpty {
			t.Logf("New group should be Empty, got %s", state.State)
			return false
		}

		// Perform random state transitions
		numTransitions := rng.Intn(20) + 5
		currentState := state.State

		for i := 0; i < numTransitions; i++ {
			// Pick a random valid transition
			validNext := validTransitions[currentState]
			if len(validNext) == 0 {
				// Dead state - no more transitions possible
				break
			}

			targetState := validNext[rng.Intn(len(validNext))]
			nowMs++

			// Try to transition
			newState, err := gs.TransitionState(ctx, groups.TransitionStateRequest{
				GroupID:      groupID,
				FromState:    currentState,
				ToState:      targetState,
				IncrementGen: targetState == groups.GroupStateCompletingRebalance,
				NowMs:        nowMs,
			})
			if err != nil {
				t.Logf("TransitionState failed: %v", err)
				return false
			}

			// Verify the transition happened correctly
			if newState.State != targetState {
				t.Logf("State mismatch: expected %s, got %s", targetState, newState.State)
				return false
			}

			currentState = targetState
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyGenerationMonotonic verifies that group generation always
// increases monotonically during rebalances.
func TestPropertyGenerationMonotonic(t *testing.T) {
	f := func(numRebalances uint8) bool {
		if numRebalances == 0 {
			return true
		}
		// Limit to avoid excessive test time
		if numRebalances > 50 {
			numRebalances = 50
		}

		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := uuid.New().String()

		// Create a group
		state, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		if state.Generation != 0 {
			t.Logf("Initial generation should be 0, got %d", state.Generation)
			return false
		}

		prevGen := state.Generation

		// Simulate multiple rebalances
		for i := uint8(0); i < numRebalances; i++ {
			nowMs++

			// Increment generation (as happens during rebalance)
			state, err = gs.IncrementGeneration(ctx, groupID, nowMs)
			if err != nil {
				t.Logf("IncrementGeneration failed: %v", err)
				return false
			}

			// Generation must be strictly greater than previous
			if state.Generation <= prevGen {
				t.Logf("Generation not monotonic: prev=%d, current=%d", prevGen, state.Generation)
				return false
			}

			// Generation must increase by exactly 1
			if state.Generation != prevGen+1 {
				t.Logf("Generation didn't increase by 1: prev=%d, current=%d", prevGen, state.Generation)
				return false
			}

			prevGen = state.Generation
		}

		// Verify final generation equals number of increments
		if state.Generation != int32(numRebalances) {
			t.Logf("Final generation mismatch: expected %d, got %d", numRebalances, state.Generation)
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

// TestPropertyMemberCountConsistent verifies that member count remains
// consistent with join/leave operations.
func TestPropertyMemberCountConsistent(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Track expected members
		expectedMembers := make(map[string]bool)
		numOps := rng.Intn(50) + 10

		for i := 0; i < numOps; i++ {
			nowMs++
			opType := rng.Intn(3) // 0=join, 1=leave, 2=heartbeat

			switch opType {
			case 0: // Join new member
				memberID := fmt.Sprintf("member-%d-%d", seed, i)
				_, err := gs.AddMember(ctx, groups.AddMemberRequest{
					GroupID:          groupID,
					MemberID:         memberID,
					ClientID:         "test-client",
					ClientHost:       "127.0.0.1",
					SessionTimeoutMs: 30000,
					NowMs:            nowMs,
				})
				if err != nil {
					t.Logf("AddMember failed: %v", err)
					return false
				}
				expectedMembers[memberID] = true

			case 1: // Leave existing member (if any)
				if len(expectedMembers) == 0 {
					continue
				}
				// Pick a random member to remove
				var memberID string
				for m := range expectedMembers {
					memberID = m
					break
				}
				err := gs.RemoveMember(ctx, groupID, memberID)
				if err != nil {
					t.Logf("RemoveMember failed: %v", err)
					return false
				}
				delete(expectedMembers, memberID)

			case 2: // Heartbeat existing member (if any)
				if len(expectedMembers) == 0 {
					continue
				}
				var memberID string
				for m := range expectedMembers {
					memberID = m
					break
				}
				err := gs.UpdateMemberHeartbeat(ctx, groupID, memberID, nowMs)
				if err != nil {
					t.Logf("UpdateMemberHeartbeat failed: %v", err)
					return false
				}
			}

			// Verify member count matches expected
			members, err := gs.ListMembers(ctx, groupID)
			if err != nil {
				t.Logf("ListMembers failed: %v", err)
				return false
			}

			if len(members) != len(expectedMembers) {
				t.Logf("Member count mismatch: expected %d, got %d", len(expectedMembers), len(members))
				return false
			}

			// Verify all expected members are present
			actualMembers := make(map[string]bool)
			for _, m := range members {
				actualMembers[m.MemberID] = true
			}
			for memberID := range expectedMembers {
				if !actualMembers[memberID] {
					t.Logf("Missing member: %s", memberID)
					return false
				}
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyConcurrentJoinLeave tests that concurrent join/leave operations
// maintain consistent state.
func TestPropertyConcurrentJoinLeave(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numWorkers := rng.Intn(5) + 2      // 2-6 workers
		opsPerWorker := rng.Intn(10) + 5   // 5-14 ops each

		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Track operations for verification
		var mu sync.Mutex
		addedMembers := make(map[string]bool)
		removedMembers := make(map[string]bool)
		var successfulAdds int64
		var successfulRemoves int64

		var wg sync.WaitGroup
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			workerID := w
			go func() {
				defer wg.Done()
				localRng := rand.New(rand.NewSource(seed + int64(workerID)))

				for i := 0; i < opsPerWorker; i++ {
					memberID := fmt.Sprintf("member-%d-%d", workerID, i)
					opNow := time.Now().UnixMilli()

					if localRng.Intn(2) == 0 {
						// Join
						_, err := gs.AddMember(ctx, groups.AddMemberRequest{
							GroupID:          groupID,
							MemberID:         memberID,
							ClientID:         fmt.Sprintf("client-%d", workerID),
							ClientHost:       "127.0.0.1",
							SessionTimeoutMs: 30000,
							NowMs:            opNow,
						})
						if err == nil {
							mu.Lock()
							addedMembers[memberID] = true
							mu.Unlock()
							atomic.AddInt64(&successfulAdds, 1)
						}
					} else {
						// Leave - try to remove any member we previously added
						mu.Lock()
						var toRemove string
						for m := range addedMembers {
							if !removedMembers[m] {
								toRemove = m
								break
							}
						}
						mu.Unlock()

						if toRemove != "" {
							err := gs.RemoveMember(ctx, groupID, toRemove)
							if err == nil {
								mu.Lock()
								removedMembers[toRemove] = true
								mu.Unlock()
								atomic.AddInt64(&successfulRemoves, 1)
							}
						}
					}
				}
			}()
		}

		wg.Wait()

		// Verify final state is consistent
		members, err := gs.ListMembers(ctx, groupID)
		if err != nil {
			t.Logf("ListMembers failed: %v", err)
			return false
		}

		// Count expected remaining members
		mu.Lock()
		expectedCount := 0
		for m := range addedMembers {
			if !removedMembers[m] {
				expectedCount++
			}
		}
		mu.Unlock()

		if len(members) != expectedCount {
			t.Logf("Final member count mismatch: expected %d, got %d (adds=%d, removes=%d)",
				expectedCount, len(members), successfulAdds, successfulRemoves)
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 30}); err != nil {
		t.Error(err)
	}
}

// TestPropertyHeartbeatPreservesMemberState verifies that heartbeats only
// update the last heartbeat time and preserve all other member fields.
func TestPropertyHeartbeatPreservesMemberState(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Add a member with specific metadata
		memberID := fmt.Sprintf("member-%d", seed)
		clientID := fmt.Sprintf("client-%d", seed)
		clientHost := fmt.Sprintf("192.168.1.%d", seed%255)
		sessionTimeout := int32(30000 + rng.Intn(30000))
		rebalanceTimeout := int32(60000 + rng.Intn(30000))
		metadata := []byte(fmt.Sprintf("metadata-%d", seed))

		original, err := gs.AddMember(ctx, groups.AddMemberRequest{
			GroupID:            groupID,
			MemberID:           memberID,
			ClientID:           clientID,
			ClientHost:         clientHost,
			SessionTimeoutMs:   sessionTimeout,
			RebalanceTimeoutMs: rebalanceTimeout,
			Metadata:           metadata,
			NowMs:              nowMs,
		})
		if err != nil {
			return false
		}

		// Perform multiple heartbeats with increasing times
		numHeartbeats := rng.Intn(20) + 5
		for i := 0; i < numHeartbeats; i++ {
			nowMs += int64(rng.Intn(1000) + 100)
			err := gs.UpdateMemberHeartbeat(ctx, groupID, memberID, nowMs)
			if err != nil {
				t.Logf("UpdateMemberHeartbeat failed: %v", err)
				return false
			}
		}

		// Verify member state is preserved except lastHeartbeatMs
		updated, err := gs.GetMember(ctx, groupID, memberID)
		if err != nil {
			t.Logf("GetMember failed: %v", err)
			return false
		}

		if updated.MemberID != original.MemberID {
			t.Logf("MemberID changed: %s -> %s", original.MemberID, updated.MemberID)
			return false
		}
		if updated.ClientID != original.ClientID {
			t.Logf("ClientID changed: %s -> %s", original.ClientID, updated.ClientID)
			return false
		}
		if updated.ClientHost != original.ClientHost {
			t.Logf("ClientHost changed: %s -> %s", original.ClientHost, updated.ClientHost)
			return false
		}
		if updated.SessionTimeoutMs != original.SessionTimeoutMs {
			t.Logf("SessionTimeoutMs changed: %d -> %d", original.SessionTimeoutMs, updated.SessionTimeoutMs)
			return false
		}
		if updated.RebalanceTimeoutMs != original.RebalanceTimeoutMs {
			t.Logf("RebalanceTimeoutMs changed: %d -> %d", original.RebalanceTimeoutMs, updated.RebalanceTimeoutMs)
			return false
		}
		if string(updated.Metadata) != string(original.Metadata) {
			t.Logf("Metadata changed")
			return false
		}
		if updated.JoinedAtMs != original.JoinedAtMs {
			t.Logf("JoinedAtMs changed: %d -> %d", original.JoinedAtMs, updated.JoinedAtMs)
			return false
		}

		// But lastHeartbeatMs should have been updated
		if updated.LastHeartbeatMs != nowMs {
			t.Logf("LastHeartbeatMs not updated: expected %d, got %d", nowMs, updated.LastHeartbeatMs)
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

// TestPropertyAssignmentConsistency verifies that assignments are correctly
// tracked and can be retrieved after being set.
func TestPropertyAssignmentConsistency(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Add some members
		numMembers := rng.Intn(5) + 2
		memberIDs := make([]string, numMembers)
		for i := 0; i < numMembers; i++ {
			memberIDs[i] = fmt.Sprintf("member-%d-%d", seed, i)
			_, err := gs.AddMember(ctx, groups.AddMemberRequest{
				GroupID:          groupID,
				MemberID:         memberIDs[i],
				ClientID:         "test-client",
				ClientHost:       "127.0.0.1",
				SessionTimeoutMs: 30000,
				NowMs:            nowMs,
			})
			if err != nil {
				return false
			}
		}

		// Set assignments for all members atomically
		expectedAssignments := make(map[string][]byte)
		for _, memberID := range memberIDs {
			assignmentData := []byte(fmt.Sprintf("assignment-for-%s-%d", memberID, rng.Int63()))
			expectedAssignments[memberID] = assignmentData
		}

		generation := int32(1)
		err = gs.SetAllAssignments(ctx, groups.SetAllAssignmentsRequest{
			GroupID:     groupID,
			Generation:  generation,
			Assignments: expectedAssignments,
			NowMs:       nowMs,
		})
		if err != nil {
			t.Logf("SetAllAssignments failed: %v", err)
			return false
		}

		// Verify each member's assignment
		for _, memberID := range memberIDs {
			assignment, err := gs.GetAssignment(ctx, groupID, memberID)
			if err != nil {
				t.Logf("GetAssignment failed for %s: %v", memberID, err)
				return false
			}
			if assignment == nil {
				t.Logf("Assignment not found for %s", memberID)
				return false
			}
			if string(assignment.Data) != string(expectedAssignments[memberID]) {
				t.Logf("Assignment data mismatch for %s", memberID)
				return false
			}
			if assignment.Generation != generation {
				t.Logf("Assignment generation mismatch for %s: expected %d, got %d",
					memberID, generation, assignment.Generation)
				return false
			}
		}

		// List all assignments and verify count
		assignments, err := gs.ListAssignments(ctx, groupID)
		if err != nil {
			t.Logf("ListAssignments failed: %v", err)
			return false
		}
		if len(assignments) != numMembers {
			t.Logf("Assignment count mismatch: expected %d, got %d", numMembers, len(assignments))
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyLeaseExclusivity verifies that only one broker can hold
// a lease at a time.
func TestPropertyLeaseExclusivity(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numBrokers := rng.Intn(5) + 2 // 2-6 brokers
		numAttempts := rng.Intn(20) + 10

		store := metadata.NewMockStore()
		ctx := context.Background()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create lease managers for multiple brokers
		leaseManagers := make([]*groups.LeaseManager, numBrokers)
		for i := 0; i < numBrokers; i++ {
			leaseManagers[i] = groups.NewLeaseManager(store, fmt.Sprintf("broker-%d", i))
		}

		// Track which broker holds the lease
		var mu sync.Mutex
		var currentHolder int = -1 // -1 means no holder

		for i := 0; i < numAttempts; i++ {
			// Random broker tries to acquire
			brokerIdx := rng.Intn(numBrokers)
			lm := leaseManagers[brokerIdx]

			result, err := lm.AcquireLease(ctx, groupID)
			if err != nil {
				t.Logf("AcquireLease failed: %v", err)
				return false
			}

			mu.Lock()
			if result.Acquired {
				// Verify lease info
				if result.Lease == nil {
					t.Logf("Acquired but lease is nil")
					mu.Unlock()
					return false
				}
				if result.Lease.BrokerID != fmt.Sprintf("broker-%d", brokerIdx) {
					t.Logf("Lease holder mismatch: expected broker-%d, got %s",
						brokerIdx, result.Lease.BrokerID)
					mu.Unlock()
					return false
				}
				currentHolder = brokerIdx
			} else {
				// Lease held by another - verify it matches current holder
				if currentHolder >= 0 {
					expectedHolder := fmt.Sprintf("broker-%d", currentHolder)
					if result.Lease.BrokerID != expectedHolder {
						t.Logf("Reported holder mismatch: expected %s, got %s",
							expectedHolder, result.Lease.BrokerID)
						mu.Unlock()
						return false
					}
				}
			}
			mu.Unlock()

			// Occasionally release the lease
			if rng.Intn(5) == 0 && currentHolder >= 0 {
				mu.Lock()
				releaseBroker := currentHolder
				mu.Unlock()

				err := leaseManagers[releaseBroker].ReleaseLease(ctx, groupID)
				if err != nil {
					t.Logf("ReleaseLease failed: %v", err)
					return false
				}

				mu.Lock()
				currentHolder = -1
				mu.Unlock()
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 30}); err != nil {
		t.Error(err)
	}
}

// TestPropertyConcurrentLeaseAcquisition tests that concurrent lease
// acquisition maintains exclusivity.
func TestPropertyConcurrentLeaseAcquisition(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numBrokers := rng.Intn(8) + 2 // 2-9 brokers
		attemptsPerBroker := rng.Intn(10) + 5

		store := metadata.NewMockStore()
		ctx := context.Background()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create lease managers for multiple brokers
		leaseManagers := make([]*groups.LeaseManager, numBrokers)
		for i := 0; i < numBrokers; i++ {
			leaseManagers[i] = groups.NewLeaseManager(store, fmt.Sprintf("broker-%d", i))
		}

		// Concurrently attempt to acquire leases
		type result struct {
			brokerIdx int
			acquired  bool
		}
		results := make(chan result, numBrokers*attemptsPerBroker)

		var wg sync.WaitGroup
		for i := 0; i < numBrokers; i++ {
			wg.Add(1)
			brokerIdx := i
			go func() {
				defer wg.Done()
				localRng := rand.New(rand.NewSource(seed + int64(brokerIdx)))
				for j := 0; j < attemptsPerBroker; j++ {
					r, err := leaseManagers[brokerIdx].AcquireLease(ctx, groupID)
					if err != nil {
						continue
					}
					results <- result{brokerIdx: brokerIdx, acquired: r.Acquired}

					// Small delay to allow interleaving
					time.Sleep(time.Microsecond * time.Duration(localRng.Intn(100)))
				}
			}()
		}

		wg.Wait()
		close(results)

		// Count acquisitions per broker
		acquisitions := make(map[int]int)
		for r := range results {
			if r.acquired {
				acquisitions[r.brokerIdx]++
			}
		}

		// At least one broker should have acquired (may be multiple times via renewal)
		totalAcquisitions := 0
		for _, count := range acquisitions {
			totalAcquisitions += count
		}
		if totalAcquisitions == 0 {
			return false
		}

		// Verify final lease state - use the first lease manager to get the actual lease
		// All lease managers share the same store, so GetLease should return the same result
		lease, err := leaseManagers[0].GetLease(ctx, groupID)
		if err != nil {
			return false
		}

		// The lease may or may not exist (if released) - either is valid
		// But if it exists, there should only be one holder
		if lease != nil {
			// Verify the holder is one of our brokers
			found := false
			for i := 0; i < numBrokers; i++ {
				if lease.BrokerID == fmt.Sprintf("broker-%d", i) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 30}); err != nil {
		t.Error(err)
	}
}

// TestPropertyGroupDeletionCleansUp verifies that deleting a group removes
// all associated data.
func TestPropertyGroupDeletionCleansUp(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Add some members
		numMembers := rng.Intn(5) + 2
		memberIDs := make([]string, numMembers)
		for i := 0; i < numMembers; i++ {
			memberIDs[i] = fmt.Sprintf("member-%d-%d", seed, i)
			_, err := gs.AddMember(ctx, groups.AddMemberRequest{
				GroupID:          groupID,
				MemberID:         memberIDs[i],
				ClientID:         "test-client",
				ClientHost:       "127.0.0.1",
				SessionTimeoutMs: 30000,
				NowMs:            nowMs,
			})
			if err != nil {
				return false
			}
		}

		// Set assignments
		assignments := make(map[string][]byte)
		for _, memberID := range memberIDs {
			assignments[memberID] = []byte(fmt.Sprintf("assignment-%s", memberID))
		}
		err = gs.SetAllAssignments(ctx, groups.SetAllAssignmentsRequest{
			GroupID:     groupID,
			Generation:  1,
			Assignments: assignments,
			NowMs:       nowMs,
		})
		if err != nil {
			return false
		}

		// Verify group exists
		exists, err := gs.GroupExists(ctx, groupID)
		if err != nil {
			return false
		}
		if !exists {
			t.Logf("Group should exist before deletion")
			return false
		}

		// Delete the group
		err = gs.DeleteGroup(ctx, groupID)
		if err != nil {
			t.Logf("DeleteGroup failed: %v", err)
			return false
		}

		// Verify group no longer exists
		exists, err = gs.GroupExists(ctx, groupID)
		if err != nil {
			return false
		}
		if exists {
			t.Logf("Group should not exist after deletion")
			return false
		}

		// Verify state is gone
		_, err = gs.GetGroupState(ctx, groupID)
		if !errors.Is(err, groups.ErrGroupNotFound) {
			t.Logf("Expected ErrGroupNotFound for state, got: %v", err)
			return false
		}

		// Verify members are gone
		members, err := gs.ListMembers(ctx, groupID)
		if err != nil {
			t.Logf("ListMembers failed: %v", err)
			return false
		}
		if len(members) != 0 {
			t.Logf("Members should be empty after deletion, got %d", len(members))
			return false
		}

		// Verify assignments are gone
		assignmentList, err := gs.ListAssignments(ctx, groupID)
		if err != nil {
			t.Logf("ListAssignments failed: %v", err)
			return false
		}
		if len(assignmentList) != 0 {
			t.Logf("Assignments should be empty after deletion, got %d", len(assignmentList))
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyOffsetCommitPersistence verifies that committed offsets
// are correctly persisted and retrievable.
func TestPropertyOffsetCommitPersistence(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Commit offsets for multiple partitions
		numTopics := rng.Intn(3) + 1
		numPartitions := rng.Intn(5) + 1
		expectedOffsets := make(map[string]int64)

		for ti := 0; ti < numTopics; ti++ {
			topic := fmt.Sprintf("topic-%d", ti)
			for p := 0; p < numPartitions; p++ {
				offset := rng.Int63n(1000000)
				key := fmt.Sprintf("%s:%d", topic, p)
				expectedOffsets[key] = offset

				_, err := gs.CommitOffset(ctx, groups.CommitOffsetRequest{
					GroupID:     groupID,
					Topic:       topic,
					Partition:   int32(p),
					Offset:      offset,
					LeaderEpoch: int32(rng.Intn(100)),
					Metadata:    fmt.Sprintf("meta-%d-%d", ti, p),
					NowMs:       nowMs,
				})
				if err != nil {
					return false
				}
			}
		}

		// Verify each offset
		for ti := 0; ti < numTopics; ti++ {
			topic := fmt.Sprintf("topic-%d", ti)
			for p := 0; p < numPartitions; p++ {
				key := fmt.Sprintf("%s:%d", topic, p)
				expected := expectedOffsets[key]

				committed, err := gs.GetCommittedOffset(ctx, groupID, topic, int32(p))
				if err != nil {
					return false
				}
				if committed == nil {
					return false
				}
				if committed.Offset != expected {
					return false
				}
			}
		}

		// Verify list returns all offsets
		allOffsets, err := gs.ListCommittedOffsets(ctx, groupID)
		if err != nil {
			return false
		}
		if len(allOffsets) != numTopics*numPartitions {
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyConcurrentMemberOperations tests that concurrent member
// operations on the same group maintain consistency.
func TestPropertyConcurrentMemberOperations(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numWorkers := rng.Intn(4) + 2
		opsPerWorker := rng.Intn(15) + 5

		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := fmt.Sprintf("test-group-%d", seed)

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		// Track operations atomically
		var mu sync.Mutex
		activeMembers := make(map[string]bool)
		var totalAdds, totalRemoves int64

		var wg sync.WaitGroup
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			workerID := w
			go func() {
				defer wg.Done()
				localRng := rand.New(rand.NewSource(seed + int64(workerID)))

				for i := 0; i < opsPerWorker; i++ {
					opNow := time.Now().UnixMilli()
					op := localRng.Intn(3)

					switch op {
					case 0: // Join
						memberID := fmt.Sprintf("member-%d-%d", workerID, i)
						_, err := gs.AddMember(ctx, groups.AddMemberRequest{
							GroupID:          groupID,
							MemberID:         memberID,
							ClientID:         fmt.Sprintf("client-%d", workerID),
							ClientHost:       "127.0.0.1",
							SessionTimeoutMs: 30000,
							NowMs:            opNow,
						})
						if err == nil {
							mu.Lock()
							activeMembers[memberID] = true
							mu.Unlock()
							atomic.AddInt64(&totalAdds, 1)
						}

					case 1: // Leave
						mu.Lock()
						var toRemove string
						for m := range activeMembers {
							toRemove = m
							break
						}
						if toRemove != "" {
							delete(activeMembers, toRemove)
						}
						mu.Unlock()

						if toRemove != "" {
							err := gs.RemoveMember(ctx, groupID, toRemove)
							if err == nil {
								atomic.AddInt64(&totalRemoves, 1)
							}
						}

					case 2: // Heartbeat
						mu.Lock()
						var toHeartbeat string
						for m := range activeMembers {
							toHeartbeat = m
							break
						}
						mu.Unlock()

						if toHeartbeat != "" {
							_ = gs.UpdateMemberHeartbeat(ctx, groupID, toHeartbeat, opNow)
						}
					}
				}
			}()
		}

		wg.Wait()

		// Verify final state
		members, err := gs.ListMembers(ctx, groupID)
		if err != nil {
			t.Logf("ListMembers failed: %v", err)
			return false
		}

		mu.Lock()
		expectedCount := len(activeMembers)
		mu.Unlock()

		if len(members) != expectedCount {
			t.Logf("Final member count mismatch: expected %d, got %d", expectedCount, len(members))
			return false
		}

		// Verify each expected member exists
		actualIDs := make(map[string]bool)
		for _, m := range members {
			actualIDs[m.MemberID] = true
		}

		mu.Lock()
		for memberID := range activeMembers {
			if !actualIDs[memberID] {
				t.Logf("Missing member: %s", memberID)
				mu.Unlock()
				return false
			}
		}
		mu.Unlock()

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 30}); err != nil {
		t.Error(err)
	}
}

// TestPropertyRandomOperationSequence tests that random sequences of
// group operations maintain all invariants.
func TestPropertyRandomOperationSequence(t *testing.T) {
	f := func(ops []GroupOp) bool {
		if len(ops) == 0 {
			return true
		}
		if len(ops) > 100 {
			ops = ops[:100]
		}

		store := metadata.NewMockStore()
		gs := groups.NewStore(store)
		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		groupID := "test-group"

		// Create a group
		_, err := gs.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      groupID,
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			return false
		}

		memberCounter := 0
		activeMembers := make(map[string]bool)

		for _, op := range ops {
			nowMs++

			switch op.OpType {
			case 0: // Join
				memberID := fmt.Sprintf("member-%d", memberCounter)
				memberCounter++
				_, err := gs.AddMember(ctx, groups.AddMemberRequest{
					GroupID:          groupID,
					MemberID:         memberID,
					ClientID:         "test-client",
					ClientHost:       "127.0.0.1",
					SessionTimeoutMs: 30000,
					NowMs:            nowMs,
				})
				if err == nil {
					activeMembers[memberID] = true
				}

			case 1: // Leave
				if len(activeMembers) == 0 {
					continue
				}
				// Get a sorted list for determinism
				var members []string
				for m := range activeMembers {
					members = append(members, m)
				}
				sort.Strings(members)
				memberID := members[0]

				err := gs.RemoveMember(ctx, groupID, memberID)
				if err == nil {
					delete(activeMembers, memberID)
				}

			case 2: // Heartbeat
				if len(activeMembers) == 0 {
					continue
				}
				var members []string
				for m := range activeMembers {
					members = append(members, m)
				}
				sort.Strings(members)
				memberID := members[0]

				_ = gs.UpdateMemberHeartbeat(ctx, groupID, memberID, nowMs)
			}
		}

		// Verify final state invariants
		members, err := gs.ListMembers(ctx, groupID)
		if err != nil {
			return false
		}

		// Member count matches
		if len(members) != len(activeMembers) {
			t.Logf("Member count mismatch: expected %d, got %d", len(activeMembers), len(members))
			return false
		}

		// All members have valid timestamps
		for _, m := range members {
			if m.JoinedAtMs <= 0 {
				t.Logf("Invalid JoinedAtMs for %s: %d", m.MemberID, m.JoinedAtMs)
				return false
			}
			if m.LastHeartbeatMs <= 0 {
				t.Logf("Invalid LastHeartbeatMs for %s: %d", m.MemberID, m.LastHeartbeatMs)
				return false
			}
			if m.LastHeartbeatMs < m.JoinedAtMs {
				t.Logf("LastHeartbeatMs < JoinedAtMs for %s", m.MemberID)
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}
