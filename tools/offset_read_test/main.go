package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "", "Topic name (required)")
	partition := flag.Int("partition", 0, "Partition number")
	offset := flag.Int64("offset", 0, "Offset to start reading from")
	maxRecords := flag.Int("max", 10, "Maximum records to read")
	timeout := flag.Duration("timeout", 15*time.Second, "Overall timeout")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: -topic is required")
		os.Exit(2)
	}
	if *maxRecords <= 0 {
		fmt.Fprintln(os.Stderr, "error: -max must be > 0")
		os.Exit(2)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*broker),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			*topic: {int32(*partition): kgo.NewOffset().At(*offset)},
		}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: creating client: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	seen := 0
	for seen < *maxRecords {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			if err == context.DeadlineExceeded {
				fmt.Fprintf(os.Stderr, "timeout after %s, saw %d records\n", timeout.String(), seen)
				return
			}
			fmt.Fprintf(os.Stderr, "fetch error: %v\n", err)
			os.Exit(1)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if seen >= *maxRecords {
				return
			}
			fmt.Printf("offset=%d timestamp=%s key=%dB value=%dB\n",
				r.Offset,
				r.Timestamp.UTC().Format(time.RFC3339),
				len(r.Key),
				len(r.Value),
			)
			seen++
		})
	}
}
