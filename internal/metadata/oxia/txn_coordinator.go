package oxia

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	"google.golang.org/grpc/metadata"
)

type txnCoordinator struct {
	namespace      string
	serviceAddress string
	requestTimeout time.Duration

	clientPool   rpc.ClientPool
	shardManager *txnShardManager
}

func newTxnCoordinator(ctx context.Context, cfg Config) (*txnCoordinator, error) {
	requestTimeout := cfg.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = rpc.DefaultRpcTimeout
	}

	clientPool := rpc.NewClientPool(nil, nil)
	shardManager, err := newTxnShardManager(ctx, clientPool, cfg.ServiceAddress, cfg.Namespace, requestTimeout)
	if err != nil {
		_ = clientPool.Close()
		return nil, err
	}

	return &txnCoordinator{
		namespace:      cfg.Namespace,
		serviceAddress: cfg.ServiceAddress,
		requestTimeout: requestTimeout,
		clientPool:     clientPool,
		shardManager:   shardManager,
	}, nil
}

func (c *txnCoordinator) Close() error {
	if c == nil {
		return nil
	}
	shardErr := c.shardManager.Close()
	poolErr := c.clientPool.Close()
	if shardErr != nil {
		return shardErr
	}
	return poolErr
}

func (c *txnCoordinator) shardForKey(key string) (int64, error) {
	return c.shardManager.shardForKey(key)
}

func (c *txnCoordinator) write(ctx context.Context, shardID int64, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	leader, err := c.shardManager.leader(shardID)
	if err != nil {
		return nil, err
	}

	client, err := c.clientPool.GetClientRpc(leader)
	if err != nil {
		return nil, err
	}

	request.Shard = &shardID
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataNamespace, c.namespace)
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataShardId, fmt.Sprintf("%d", shardID))
	return client.Write(ctx, request)
}

type txnShardManager struct {
	mu             sync.RWMutex
	hashFunc       func(string) uint32
	shards         map[int64]txnShard
	namespace      string
	serviceAddress string
	clientPool     rpc.ClientPool
	requestTimeout time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	readyOnce      sync.Once
	readyCh        chan struct{}
}

type txnShard struct {
	id        int64
	leader    string
	hashRange hashRange
}

type hashRange struct {
	minInclusive uint32
	maxInclusive uint32
}

func newTxnShardManager(ctx context.Context, clientPool rpc.ClientPool, serviceAddress, namespace string, requestTimeout time.Duration) (*txnShardManager, error) {
	sm := &txnShardManager{
		hashFunc:       hash.Xxh332,
		shards:         make(map[int64]txnShard),
		namespace:      namespace,
		serviceAddress: serviceAddress,
		clientPool:     clientPool,
		requestTimeout: requestTimeout,
		readyCh:        make(chan struct{}),
	}
	sm.ctx, sm.cancel = context.WithCancel(ctx)

	go sm.receiveLoop()

	waitCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err := sm.waitReady(waitCtx); err != nil {
		sm.cancel()
		return nil, err
	}
	return sm, nil
}

func (sm *txnShardManager) Close() error {
	if sm == nil {
		return nil
	}
	sm.cancel()
	return nil
}

func (sm *txnShardManager) waitReady(ctx context.Context) error {
	select {
	case <-sm.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sm *txnShardManager) receiveLoop() {
	retryDelay := 200 * time.Millisecond
	for {
		if err := sm.receive(); err != nil {
			if errors.Is(err, context.Canceled) || sm.ctx.Err() != nil {
				return
			}
			select {
			case <-time.After(retryDelay):
				continue
			case <-sm.ctx.Done():
				return
			}
		}
	}
}

func (sm *txnShardManager) receive() error {
	client, err := sm.clientPool.GetClientRpc(sm.serviceAddress)
	if err != nil {
		return err
	}

	stream, err := client.GetShardAssignments(sm.ctx, &proto.ShardAssignmentsRequest{Namespace: sm.namespace})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		assignments, ok := resp.Namespaces[sm.namespace]
		if !ok {
			continue
		}
		if assignments.ShardKeyRouter != proto.ShardKeyRouter_XXHASH3 {
			return fmt.Errorf("oxia: unsupported shard key router %v", assignments.ShardKeyRouter)
		}

		shards, err := parseShardAssignments(assignments.Assignments)
		if err != nil {
			return err
		}
		sm.update(shards)
	}
}

func parseShardAssignments(assignments []*proto.ShardAssignment) ([]txnShard, error) {
	shards := make([]txnShard, 0, len(assignments))
	for _, assignment := range assignments {
		rng, err := parseShardRange(assignment)
		if err != nil {
			return nil, err
		}
		shards = append(shards, txnShard{
			id:        assignment.Shard,
			leader:    assignment.Leader,
			hashRange: rng,
		})
	}
	return shards, nil
}

func parseShardRange(assignment *proto.ShardAssignment) (hashRange, error) {
	switch boundaries := assignment.ShardBoundaries.(type) {
	case *proto.ShardAssignment_Int32HashRange:
		return hashRange{
			minInclusive: boundaries.Int32HashRange.MinHashInclusive,
			maxInclusive: boundaries.Int32HashRange.MaxHashInclusive,
		}, nil
	default:
		return hashRange{}, errors.New("oxia: unknown shard boundary type")
	}
}

func (sm *txnShardManager) update(shards []txnShard) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.shards = make(map[int64]txnShard, len(shards))
	for _, shard := range shards {
		sm.shards[shard.id] = shard
	}
	sm.readyOnce.Do(func() {
		close(sm.readyCh)
	})
}

func (sm *txnShardManager) shardForKey(key string) (int64, error) {
	hashCode := sm.hashFunc(key)

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, shard := range sm.shards {
		if shard.hashRange.minInclusive <= hashCode && hashCode <= shard.hashRange.maxInclusive {
			return shard.id, nil
		}
	}
	return 0, errors.New("oxia: shard not found for key")
}

func (sm *txnShardManager) leader(shardID int64) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	shard, ok := sm.shards[shardID]
	if !ok {
		return "", errors.New("oxia: shard leader not found")
	}
	return shard.leader, nil
}
