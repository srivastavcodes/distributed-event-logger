package log

import (
	"context"
	api "proglog/api/v1"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger *zap.Logger
	mu     sync.Mutex

	servers map[string]chan struct{}
	close   chan struct{}
	closed  bool
}

func (rep *Replicator) init() {
	if rep.logger == nil {
		rep.logger = zap.L().Named("replicator")
	}
	if rep.close == nil {
		rep.close = make(chan struct{})
	}
	if rep.servers == nil {
		rep.servers = make(map[string]chan struct{})
	}
}

// Join adds the given server address to the list of servers to replicate, and kicks
// off the replicate goroutine to the run the actual replication logic
func (rep *Replicator) Join(name string, addr string) error {
	rep.mu.Lock()
	defer rep.mu.Unlock()

	rep.init()
	if rep.closed {
		return nil
	}
	if _, ok := rep.servers[name]; ok {
		// already replicating so skip
		return nil
	}
	rep.servers[name] = make(chan struct{})

	go rep.replicate(addr, rep.servers[name])
	return nil
}

// replicate establishes a connection to the specified addr and continuously streams (reads)
// records from it, forwarding each received record to the local server for replication.
// It runs in a separate goroutine and stops when the replicator closes or
// when the server leaves the cluster, ensuring data consistency across nodes.
func (rep *Replicator) replicate(addr string, leave chan struct{}) {
	conn, err := grpc.NewClient(addr, rep.DialOptions...)
	if err != nil {
		rep.logError(err, "failed to dial", addr)
		return
	}
	defer conn.Close()
	client := api.NewLogClient(conn)

	stream, err := client.ConsumeStream(
		context.Background(),
		&api.ConsumeRequest{Offset: 0},
	)
	if err != nil {
		rep.logError(err, "failed to consume", addr)
		return
	}
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				rep.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()
	for {
		select {
		case <-rep.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = rep.LocalServer.Produce(context.Background(),
				&api.ProduceRequest{Record: record},
			)
			if err != nil {
				rep.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave removes a server from replication by closing its rep.servers[name] channel.
// It signals the replicate goroutine to stop streaming data from that server and
// removes the server entry from the Replicator's server map.
func (rep *Replicator) Leave(name string) error {
	rep.mu.Lock()
	defer rep.mu.Unlock()

	rep.init()
	if _, ok := rep.servers[name]; !ok {
		return nil
	}
	close(rep.servers[name])

	delete(rep.servers, name)
	return nil
}

func (rep *Replicator) Close() error {
	rep.mu.Lock()
	defer rep.mu.Unlock()

	rep.init()
	if rep.closed {
		return nil
	}
	rep.closed = true

	close(rep.close)
	return nil
}

func (rep *Replicator) logError(err error, msg string, addr string) {
	rep.logger.Error(msg, zap.String("addr", addr), zap.Error(err))
}
