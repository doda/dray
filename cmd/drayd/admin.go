package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/oxia"
	"github.com/dray-io/dray/internal/topics"
)

// AdminOptions contains configuration for admin commands.
type AdminOptions struct {
	Config     *config.Config
	Logger     *logging.Logger
	MetaStore  metadata.MetadataStore
	TopicStore *topics.Store
	GroupStore *groups.Store
	Stream     *index.StreamManager
}

// runAdmin handles admin subcommands.
func runAdmin(args []string) {
	if len(args) < 1 {
		printAdminUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	switch subcommand {
	case "topics":
		runAdminTopics(args[1:])
	case "groups":
		runAdminGroups(args[1:])
	case "configs":
		runAdminConfigs(args[1:])
	case "status":
		runAdminStatus(args[1:])
	case "help", "-h", "--help":
		printAdminUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown admin command: %s\n\n", subcommand)
		printAdminUsage()
		os.Exit(1)
	}
}

func printAdminUsage() {
	fmt.Println(`Usage: drayd admin <command> [options]

Admin commands for managing Dray cluster.

Commands:
  topics     Topic management (list, describe, create, delete)
  groups     Consumer group management (list, describe, delete)
  configs    Configuration management (describe, alter)
  status     Cluster status and diagnostics

Run 'drayd admin <command> --help' for more information on a command.`)
}

// ============================================================================
// Topic Commands
// ============================================================================

func runAdminTopics(args []string) {
	if len(args) < 1 {
		printTopicsUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	switch subcommand {
	case "list":
		runTopicsList(args[1:])
	case "describe":
		runTopicsDescribe(args[1:])
	case "create":
		runTopicsCreate(args[1:])
	case "delete":
		runTopicsDelete(args[1:])
	case "help", "-h", "--help":
		printTopicsUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown topics command: %s\n\n", subcommand)
		printTopicsUsage()
		os.Exit(1)
	}
}

func printTopicsUsage() {
	fmt.Println(`Usage: drayd admin topics <command> [options]

Topic management commands.

Commands:
  list       List all topics
  describe   Describe a specific topic
  create     Create a new topic
  delete     Delete a topic

Run 'drayd admin topics <command> --help' for more information.`)
}

func runTopicsList(args []string) {
	fs := flag.NewFlagSet("topics list", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin topics list [options]

List all topics.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topicList, err := opts.TopicStore.ListTopics(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing topics: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		data, _ := json.MarshalIndent(topicList, "", "  ")
		fmt.Println(string(data))
		return
	}

	if len(topicList) == 0 {
		fmt.Println("No topics found.")
		return
	}

	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].Name < topicList[j].Name
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPARTITIONS\tTOPIC_ID")
	for _, t := range topicList {
		fmt.Fprintf(w, "%s\t%d\t%s\n", t.Name, t.PartitionCount, t.TopicID)
	}
	w.Flush()
}

func runTopicsDescribe(args []string) {
	fs := flag.NewFlagSet("topics describe", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin topics describe [options] <topic>

Describe a specific topic.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: topic name required")
		fs.Usage()
		os.Exit(1)
	}

	topicName := fs.Arg(0)

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic, err := opts.TopicStore.GetTopic(ctx, topicName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			fmt.Fprintf(os.Stderr, "error: topic '%s' not found\n", topicName)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	partitions, err := opts.TopicStore.ListPartitions(ctx, topicName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing partitions: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		output := map[string]any{
			"topic":      topic,
			"partitions": partitions,
		}
		data, _ := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(data))
		return
	}

	fmt.Printf("Topic: %s\n", topic.Name)
	fmt.Printf("  Topic ID: %s\n", topic.TopicID)
	fmt.Printf("  Partitions: %d\n", topic.PartitionCount)
	fmt.Printf("  Created: %s\n", time.UnixMilli(topic.CreatedAtMs).Format(time.RFC3339))

	if len(topic.Config) > 0 {
		fmt.Println("  Configs:")
		keys := make([]string, 0, len(topic.Config))
		for k := range topic.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("    %s = %s\n", k, topic.Config[k])
		}
	}

	if len(partitions) > 0 {
		fmt.Println("  Partition Details:")
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].Partition < partitions[j].Partition
		})
		for _, p := range partitions {
			fmt.Printf("    [%d] StreamID: %s, State: %s\n", p.Partition, p.StreamID, p.State)
		}
	}
}

func runTopicsCreate(args []string) {
	fs := flag.NewFlagSet("topics create", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	partitions := fs.Int("partitions", 1, "Number of partitions")
	configStr := fs.String("config-values", "", "Comma-separated config key=value pairs")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin topics create [options] <topic>

Create a new topic.

Options:`)
		fs.PrintDefaults()
		fmt.Println(`
Examples:
  drayd admin topics create my-topic
  drayd admin topics create --partitions 10 my-topic
  drayd admin topics create --config-values "retention.ms=86400000,cleanup.policy=delete" my-topic`)
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: topic name required")
		fs.Usage()
		os.Exit(1)
	}

	topicName := fs.Arg(0)

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	// Parse config values
	configMap := make(map[string]string)
	if *configStr != "" {
		pairs := strings.Split(*configStr, ",")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				fmt.Fprintf(os.Stderr, "error: invalid config format '%s', expected key=value\n", pair)
				os.Exit(1)
			}
			configMap[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: int32(*partitions),
		Config:         configMap,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		if errors.Is(err, topics.ErrTopicExists) {
			fmt.Fprintf(os.Stderr, "error: topic '%s' already exists\n", topicName)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error creating topic: %v\n", err)
		os.Exit(1)
	}

	// Initialize streams for each partition
	for _, p := range result.Partitions {
		err := opts.Stream.CreateStreamWithID(ctx, p.StreamID, topicName, p.Partition)
		if err != nil && !errors.Is(err, index.ErrStreamExists) {
			fmt.Fprintf(os.Stderr, "warning: failed to initialize stream for partition %d: %v\n", p.Partition, err)
		}
	}

	if *jsonOutput {
		data, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(data))
		return
	}

	fmt.Printf("Created topic '%s' with %d partition(s)\n", topicName, *partitions)
	fmt.Printf("  Topic ID: %s\n", result.Topic.TopicID)
}

func runTopicsDelete(args []string) {
	fs := flag.NewFlagSet("topics delete", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	force := fs.Bool("force", false, "Skip confirmation prompt")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin topics delete [options] <topic>

Delete a topic.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: topic name required")
		fs.Usage()
		os.Exit(1)
	}

	topicName := fs.Arg(0)

	if !*force {
		fmt.Printf("Delete topic '%s'? This action cannot be undone. [y/N]: ", topicName)
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Aborted.")
			os.Exit(0)
		}
	}

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := opts.TopicStore.DeleteTopic(ctx, topicName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			fmt.Fprintf(os.Stderr, "error: topic '%s' not found\n", topicName)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error deleting topic: %v\n", err)
		os.Exit(1)
	}

	// Mark streams for deletion
	for _, p := range result.Partitions {
		if err := opts.Stream.MarkStreamDeleted(ctx, p.StreamID); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to mark stream for deletion: %v\n", err)
		}
	}

	fmt.Printf("Deleted topic '%s'\n", topicName)
}

// ============================================================================
// Group Commands
// ============================================================================

func runAdminGroups(args []string) {
	if len(args) < 1 {
		printGroupsUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	switch subcommand {
	case "list":
		runGroupsList(args[1:])
	case "describe":
		runGroupsDescribe(args[1:])
	case "delete":
		runGroupsDelete(args[1:])
	case "help", "-h", "--help":
		printGroupsUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown groups command: %s\n\n", subcommand)
		printGroupsUsage()
		os.Exit(1)
	}
}

func printGroupsUsage() {
	fmt.Println(`Usage: drayd admin groups <command> [options]

Consumer group management commands.

Commands:
  list       List all consumer groups
  describe   Describe a specific group
  delete     Delete a consumer group

Run 'drayd admin groups <command> --help' for more information.`)
}

func runGroupsList(args []string) {
	fs := flag.NewFlagSet("groups list", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")
	stateFilter := fs.String("state", "", "Filter by state (Empty, Stable, PreparingRebalance, etc.)")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin groups list [options]

List all consumer groups.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	groupList, err := opts.GroupStore.ListGroups(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing groups: %v\n", err)
		os.Exit(1)
	}

	// Apply state filter
	if *stateFilter != "" {
		filtered := make([]groups.GroupState, 0)
		for _, g := range groupList {
			if strings.EqualFold(string(g.State), *stateFilter) {
				filtered = append(filtered, g)
			}
		}
		groupList = filtered
	}

	if *jsonOutput {
		data, _ := json.MarshalIndent(groupList, "", "  ")
		fmt.Println(string(data))
		return
	}

	if len(groupList) == 0 {
		fmt.Println("No consumer groups found.")
		return
	}

	sort.Slice(groupList, func(i, j int) bool {
		return groupList[i].GroupID < groupList[j].GroupID
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "GROUP_ID\tSTATE\tPROTOCOL_TYPE\tGENERATION")
	for _, g := range groupList {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", g.GroupID, g.State, g.ProtocolType, g.Generation)
	}
	w.Flush()
}

func runGroupsDescribe(args []string) {
	fs := flag.NewFlagSet("groups describe", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")
	showMembers := fs.Bool("members", false, "Show group members")
	showOffsets := fs.Bool("offsets", false, "Show committed offsets")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin groups describe [options] <group-id>

Describe a specific consumer group.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: group ID required")
		fs.Usage()
		os.Exit(1)
	}

	groupID := fs.Arg(0)

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	state, err := opts.GroupStore.GetGroupState(ctx, groupID)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			fmt.Fprintf(os.Stderr, "error: group '%s' not found\n", groupID)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	groupType, _ := opts.GroupStore.GetGroupType(ctx, groupID)

	var members []groups.GroupMember
	if *showMembers {
		members, err = opts.GroupStore.ListMembers(ctx, groupID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to list members: %v\n", err)
		}
	}

	var offsets []groups.CommittedOffset
	if *showOffsets {
		offsets, err = opts.GroupStore.ListCommittedOffsets(ctx, groupID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to list offsets: %v\n", err)
		}
	}

	if *jsonOutput {
		output := map[string]any{
			"state": state,
			"type":  groupType,
		}
		if *showMembers {
			output["members"] = members
		}
		if *showOffsets {
			output["offsets"] = offsets
		}
		data, _ := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(data))
		return
	}

	fmt.Printf("Group: %s\n", groupID)
	fmt.Printf("  Type: %s\n", groupType)
	fmt.Printf("  State: %s\n", state.State)
	fmt.Printf("  Generation: %d\n", state.Generation)
	if state.ProtocolType != "" {
		fmt.Printf("  Protocol Type: %s\n", state.ProtocolType)
	}
	if state.Protocol != "" {
		fmt.Printf("  Protocol: %s\n", state.Protocol)
	}
	if state.Leader != "" {
		fmt.Printf("  Leader: %s\n", state.Leader)
	}
	fmt.Printf("  Created: %s\n", time.UnixMilli(state.CreatedAtMs).Format(time.RFC3339))
	fmt.Printf("  Updated: %s\n", time.UnixMilli(state.UpdatedAtMs).Format(time.RFC3339))

	if *showMembers && len(members) > 0 {
		fmt.Printf("\n  Members (%d):\n", len(members))
		for _, m := range members {
			fmt.Printf("    - %s (client: %s, host: %s)\n", m.MemberID, m.ClientID, m.ClientHost)
			if m.GroupInstanceID != "" {
				fmt.Printf("      Instance ID: %s\n", m.GroupInstanceID)
			}
		}
	}

	if *showOffsets && len(offsets) > 0 {
		fmt.Printf("\n  Committed Offsets (%d):\n", len(offsets))
		sort.Slice(offsets, func(i, j int) bool {
			if offsets[i].Topic != offsets[j].Topic {
				return offsets[i].Topic < offsets[j].Topic
			}
			return offsets[i].Partition < offsets[j].Partition
		})
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "    TOPIC\tPARTITION\tOFFSET\tMETADATA")
		for _, o := range offsets {
			meta := o.Metadata
			if len(meta) > 30 {
				meta = meta[:27] + "..."
			}
			fmt.Fprintf(w, "    %s\t%d\t%d\t%s\n", o.Topic, o.Partition, o.Offset, meta)
		}
		w.Flush()
	}
}

func runGroupsDelete(args []string) {
	fs := flag.NewFlagSet("groups delete", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	force := fs.Bool("force", false, "Skip confirmation prompt")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin groups delete [options] <group-id>

Delete a consumer group.

Note: The group must be empty (no active members) to be deleted.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: group ID required")
		fs.Usage()
		os.Exit(1)
	}

	groupID := fs.Arg(0)

	if !*force {
		fmt.Printf("Delete consumer group '%s'? This will also delete all committed offsets. [y/N]: ", groupID)
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Aborted.")
			os.Exit(0)
		}
	}

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if group has active members
	members, err := opts.GroupStore.ListMembers(ctx, groupID)
	if err != nil && !errors.Is(err, groups.ErrGroupNotFound) {
		fmt.Fprintf(os.Stderr, "error checking group members: %v\n", err)
		os.Exit(1)
	}

	if len(members) > 0 {
		fmt.Fprintf(os.Stderr, "error: group '%s' has %d active member(s). Cannot delete non-empty group.\n", groupID, len(members))
		os.Exit(1)
	}

	// Delete committed offsets first
	if err := opts.GroupStore.DeleteAllCommittedOffsets(ctx, groupID); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to delete committed offsets: %v\n", err)
	}

	// Delete the group
	if err := opts.GroupStore.DeleteGroup(ctx, groupID); err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			fmt.Fprintf(os.Stderr, "error: group '%s' not found\n", groupID)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error deleting group: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Deleted consumer group '%s'\n", groupID)
}

// ============================================================================
// Config Commands
// ============================================================================

func runAdminConfigs(args []string) {
	if len(args) < 1 {
		printConfigsUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	switch subcommand {
	case "describe":
		runConfigsDescribe(args[1:])
	case "alter":
		runConfigsAlter(args[1:])
	case "help", "-h", "--help":
		printConfigsUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown configs command: %s\n\n", subcommand)
		printConfigsUsage()
		os.Exit(1)
	}
}

func printConfigsUsage() {
	fmt.Println(`Usage: drayd admin configs <command> [options]

Configuration management commands.

Commands:
  describe   Describe configuration for a topic
  alter      Alter configuration for a topic

Run 'drayd admin configs <command> --help' for more information.`)
}

func runConfigsDescribe(args []string) {
	fs := flag.NewFlagSet("configs describe", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")
	showDefaults := fs.Bool("defaults", false, "Include default values")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin configs describe [options] <topic>

Describe configuration for a topic.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: topic name required")
		fs.Usage()
		os.Exit(1)
	}

	topicName := fs.Arg(0)

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := opts.TopicStore.GetTopicConfig(ctx, topicName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			fmt.Fprintf(os.Stderr, "error: topic '%s' not found\n", topicName)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Add defaults if requested
	allConfigs := make(map[string]configEntry)
	if *showDefaults {
		for key, def := range topics.DefaultConfigs() {
			allConfigs[key] = configEntry{
				Value:   def,
				Source:  "DEFAULT",
				Default: true,
			}
		}
	}

	// Override with actual values
	for key, val := range cfg {
		allConfigs[key] = configEntry{
			Value:   val,
			Source:  "STATIC_TOPIC_CONFIG",
			Default: false,
		}
	}

	if *jsonOutput {
		data, _ := json.MarshalIndent(allConfigs, "", "  ")
		fmt.Println(string(data))
		return
	}

	if len(allConfigs) == 0 {
		fmt.Printf("No configuration for topic '%s'\n", topicName)
		return
	}

	keys := make([]string, 0, len(allConfigs))
	for k := range allConfigs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Printf("Configuration for topic '%s':\n", topicName)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CONFIG\tVALUE\tSOURCE")
	for _, k := range keys {
		entry := allConfigs[k]
		fmt.Fprintf(w, "%s\t%s\t%s\n", k, entry.Value, entry.Source)
	}
	w.Flush()
}

type configEntry struct {
	Value   string `json:"value"`
	Source  string `json:"source"`
	Default bool   `json:"default"`
}

func runConfigsAlter(args []string) {
	fs := flag.NewFlagSet("configs alter", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	setConfigs := fs.String("set", "", "Comma-separated key=value pairs to set")
	deleteConfigs := fs.String("delete", "", "Comma-separated config keys to delete")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin configs alter [options] <topic>

Alter configuration for a topic.

Options:`)
		fs.PrintDefaults()
		fmt.Println(`
Examples:
  drayd admin configs alter --set "retention.ms=86400000" my-topic
  drayd admin configs alter --delete "retention.ms" my-topic
  drayd admin configs alter --set "cleanup.policy=compact" --delete "retention.bytes" my-topic`)
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: topic name required")
		fs.Usage()
		os.Exit(1)
	}

	topicName := fs.Arg(0)

	if *setConfigs == "" && *deleteConfigs == "" {
		fmt.Fprintln(os.Stderr, "error: at least one of --set or --delete is required")
		fs.Usage()
		os.Exit(1)
	}

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Process set operations
	if *setConfigs != "" {
		pairs := strings.Split(*setConfigs, ",")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				fmt.Fprintf(os.Stderr, "error: invalid config format '%s', expected key=value\n", pair)
				os.Exit(1)
			}
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])

			if err := opts.TopicStore.SetTopicConfig(ctx, topicName, key, value); err != nil {
				if errors.Is(err, topics.ErrTopicNotFound) {
					fmt.Fprintf(os.Stderr, "error: topic '%s' not found\n", topicName)
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "error setting config '%s': %v\n", key, err)
				os.Exit(1)
			}
			fmt.Printf("Set %s=%s\n", key, value)
		}
	}

	// Process delete operations
	if *deleteConfigs != "" {
		keys := strings.Split(*deleteConfigs, ",")
		for _, key := range keys {
			key = strings.TrimSpace(key)
			if err := opts.TopicStore.DeleteTopicConfig(ctx, topicName, key); err != nil {
				if errors.Is(err, topics.ErrTopicNotFound) {
					fmt.Fprintf(os.Stderr, "error: topic '%s' not found\n", topicName)
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "error deleting config '%s': %v\n", key, err)
				os.Exit(1)
			}
			fmt.Printf("Deleted %s\n", key)
		}
	}

	fmt.Printf("Configuration updated for topic '%s'\n", topicName)
}

// ============================================================================
// Status Commands
// ============================================================================

func runAdminStatus(args []string) {
	fs := flag.NewFlagSet("admin status", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd admin status [options]

Show cluster status and diagnostics.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	opts, cleanup, err := initAdminOpts(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	status := ClusterStatus{
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// Count topics
	topicList, err := opts.TopicStore.ListTopics(ctx)
	if err != nil {
		status.Errors = append(status.Errors, fmt.Sprintf("failed to list topics: %v", err))
	} else {
		status.TopicCount = len(topicList)
		for _, t := range topicList {
			status.TotalPartitions += int(t.PartitionCount)
		}
	}

	// Count groups
	groupList, err := opts.GroupStore.ListGroups(ctx)
	if err != nil {
		status.Errors = append(status.Errors, fmt.Sprintf("failed to list groups: %v", err))
	} else {
		status.GroupCount = len(groupList)
		for _, g := range groupList {
			switch g.State {
			case groups.GroupStateStable:
				status.StableGroups++
			case groups.GroupStateEmpty:
				status.EmptyGroups++
			default:
				status.RebalancingGroups++
			}
		}
	}

	// Check metadata store connectivity
	_, err = opts.MetaStore.Get(ctx, "/dray/v1/ping")
	if err != nil && !errors.Is(err, metadata.ErrKeyNotFound) {
		status.MetadataStore = "error"
		status.Errors = append(status.Errors, fmt.Sprintf("metadata store: %v", err))
	} else {
		status.MetadataStore = "ok"
	}

	if *jsonOutput {
		data, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(data))
		return
	}

	fmt.Println("Cluster Status")
	fmt.Println("==============")
	fmt.Printf("Timestamp: %s\n", status.Timestamp)
	fmt.Printf("Metadata Store: %s\n", status.MetadataStore)
	fmt.Println()

	fmt.Println("Topics")
	fmt.Printf("  Total: %d\n", status.TopicCount)
	fmt.Printf("  Partitions: %d\n", status.TotalPartitions)
	fmt.Println()

	fmt.Println("Consumer Groups")
	fmt.Printf("  Total: %d\n", status.GroupCount)
	fmt.Printf("  Stable: %d\n", status.StableGroups)
	fmt.Printf("  Empty: %d\n", status.EmptyGroups)
	fmt.Printf("  Rebalancing: %d\n", status.RebalancingGroups)

	if len(status.Errors) > 0 {
		fmt.Println()
		fmt.Println("Errors:")
		for _, e := range status.Errors {
			fmt.Printf("  - %s\n", e)
		}
	}
}

// ClusterStatus holds diagnostic information about the cluster.
type ClusterStatus struct {
	Timestamp         string   `json:"timestamp"`
	MetadataStore     string   `json:"metadataStore"`
	TopicCount        int      `json:"topicCount"`
	TotalPartitions   int      `json:"totalPartitions"`
	GroupCount        int      `json:"groupCount"`
	StableGroups      int      `json:"stableGroups"`
	EmptyGroups       int      `json:"emptyGroups"`
	RebalancingGroups int      `json:"rebalancingGroups"`
	Errors            []string `json:"errors,omitempty"`
}

// ============================================================================
// Helper Functions
// ============================================================================

func initAdminOpts(configPath string) (*AdminOptions, func(), error) {
	// Load configuration without validation - admin commands can work without full config
	var cfg *config.Config
	var err error
	if configPath != "" {
		cfg, err = config.LoadFromPathNoValidate(configPath)
	} else {
		cfg, err = config.LoadNoValidate()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize metadata store based on configuration
	var metaStore metadata.MetadataStore
	if cfg.Metadata.OxiaEndpoint != "" {
		// Use Oxia store when endpoint is configured
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		namespace := cfg.Metadata.Namespace
		if namespace == "" {
			namespace = "dray"
		}

		oxiaStore, err := oxia.New(ctx, oxia.Config{
			ServiceAddress: cfg.Metadata.OxiaEndpoint,
			Namespace:      namespace,
			RequestTimeout: 30 * time.Second,
			SessionTimeout: 15 * time.Second,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to connect to Oxia at %s: %w", cfg.Metadata.OxiaEndpoint, err)
		}
		metaStore = oxiaStore
	} else {
		// Fall back to mock store when no Oxia endpoint is configured
		// This is useful for local testing or when metadata store is not available
		metaStore = metadata.NewMockStore()
	}

	// Initialize stores
	topicStore := topics.NewStore(metaStore)
	groupStore := groups.NewStore(metaStore)
	streamMgr := index.NewStreamManager(metaStore)

	cleanup := func() {
		metaStore.Close()
	}

	return &AdminOptions{
		Config:     cfg,
		MetaStore:  metaStore,
		TopicStore: topicStore,
		GroupStore: groupStore,
		Stream:     streamMgr,
	}, cleanup, nil
}

// parseInt32 parses a string to int32, returning 0 on error.
func parseInt32(s string) int32 {
	i, _ := strconv.ParseInt(s, 10, 32)
	return int32(i)
}
