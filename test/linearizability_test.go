package test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// kvInput represents an operation on the KV store.
type kvInput struct {
	Op    string `json:"op"`    // "put", "get", "delete"
	Key   string `json:"key"`
	Value string `json:"value"` // for put
}

// kvOutput represents the result of an operation.
type kvOutput struct {
	Value   string `json:"value"`   // for get
	Found   bool   `json:"found"`   // for get
	Version uint64 `json:"version"` // for put
}

// kvModel is the Porcupine linearizability model for a single-key register.
var kvModel = porcupine.Model{
	// State is the current value (string). Empty = not set.
	Init: func() interface{} { return "" },
	Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)

		switch in.Op {
		case "put":
			// Put always succeeds, new state is the written value.
			return true, in.Value
		case "get":
			if st == "" {
				// Key doesn't exist.
				return !out.Found, st
			}
			// Key exists.
			return out.Found && out.Value == st, st
		case "delete":
			return true, ""
		}
		return false, st
	},
	Equal: func(a, b interface{}) bool {
		return a.(string) == b.(string)
	},
	DescribeOperation: func(input interface{}, output interface{}) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		inJSON, _ := json.Marshal(in)
		outJSON, _ := json.Marshal(out)
		return fmt.Sprintf("%s -> %s", inJSON, outJSON)
	},
}

func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping linearizability test in short mode")
	}

	cluster := newTestCluster(t, 3)
	leader := cluster.waitForLeader(t, 10*time.Second)

	conn, err := grpc.NewClient(leader.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := pb.NewMemoryServiceClient(conn)

	const (
		numClients = 5
		opsPerClient = 20
	)

	var (
		mu         sync.Mutex
		events     []porcupine.Event
		eventID    int
	)

	addCall := func(clientID int, input kvInput) int {
		mu.Lock()
		id := eventID
		eventID++
		events = append(events, porcupine.Event{
			Kind:  porcupine.CallEvent,
			Id:    id,
			Value: input,
		})
		mu.Unlock()
		return id
	}

	addReturn := func(id int, output kvOutput) {
		mu.Lock()
		events = append(events, porcupine.Event{
			Kind:  porcupine.ReturnEvent,
			Id:    id,
			Value: output,
		})
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			ctx := context.Background()

			for i := 0; i < opsPerClient; i++ {
				switch i % 3 {
				case 0: // put
					input := kvInput{Op: "put", Key: "k", Value: fmt.Sprintf("v-%d-%d", clientID, i)}
					id := addCall(clientID, input)

					resp, err := client.Put(ctx, &pb.PutRequest{
						Namespace: "lin",
						Key:       "k",
						Value:     []byte(input.Value),
					})

					var output kvOutput
					if err == nil {
						output.Version = resp.Version
					}
					addReturn(id, output)

				case 1: // get
					input := kvInput{Op: "get", Key: "k"}
					id := addCall(clientID, input)

					resp, err := client.Get(ctx, &pb.GetRequest{
						Namespace:   "lin",
						Key:         "k",
						Consistency: pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE,
					})

					var output kvOutput
					if err == nil {
						output.Found = true
						output.Value = string(resp.Record.Value)
					}
					addReturn(id, output)

				case 2: // delete
					input := kvInput{Op: "delete", Key: "k"}
					id := addCall(clientID, input)

					_, _ = client.Delete(ctx, &pb.DeleteRequest{
						Namespace: "lin",
						Key:       "k",
					})
					addReturn(id, kvOutput{})
				}
			}
		}(c)
	}

	wg.Wait()

	ok := porcupine.CheckEvents(kvModel, events)
	if !ok {
		t.Fatal("linearizability check failed")
	}
}
