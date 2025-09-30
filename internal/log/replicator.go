package log

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer protolog.LogClient

	logger *zerolog.Logger

	mu          sync.Mutex
	serversChan map[string]chan struct{}
	closed      bool
	closeChan   chan struct{}
}

// Join method adds the given server address to the list of servers to
// replicate and kicks off the add goroutine to run the replication
// logic.
func (r *Replicator) Join(name string, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.init()
	if _, ok := r.serversChan[name]; ok {
		// already replicating so skip
		return nil
	}
	r.serversChan[name] = make(chan struct{})

	go r.replicate(addr, r.serversChan[name])
	return nil
}

// replicate consumes the log from the discovered server in a stream and then
// produces to the local server to save a copy.
//
// The messages are replicated from other servers until that server fails or
// leaves the cluster and the Replicator closes the channel for that server,
// which will end the replicate goroutine.
func (r *Replicator) replicate(addr string, leaveChan chan struct{}) {
	conn, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
	}
	defer conn.Close()

	client := protolog.NewLogClient(conn)

	ctx := context.Background()
	consReq := &protolog.ConsumeRequest{
		Offset: 0,
	}
	stream, err := client.ConsumeStream(ctx, consReq)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}
	records := make(chan *protolog.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()
	for {
		select {
		case <-r.closeChan:
			return
		case <-leaveChan:
			return
		case record := <-records:
			prodReq := &protolog.ProduceRequest{
				Record: record,
			}
			_, err := r.LocalServer.Produce(ctx, prodReq)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave method handles the server leaving the cluster by removing the server
// from the list of servers to replicate from and close the server's associated
// channel (the signal to stop replicating).
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.serversChan[name]; !ok {
		return nil
	}
	close(r.serversChan[name])
	delete(r.serversChan, name)
	return nil
}

// logError logs the error. TODO: expose an error channel to users.
func (r *Replicator) logError(err error, msg string, addr string) {
	r.logger.Error().Err(err).
		Str("addr", addr).Msg(msg)
}

// init lazily initializes the server map.
func (r *Replicator) init() {
	if r.logger == nil {
		*r.logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	}
	if r.serversChan == nil {
		r.serversChan = make(map[string]chan struct{})
	}
	if r.closeChan == nil {
		r.closeChan = make(chan struct{})
	}
}

// Close closes the replicator so it doesn't replicate new servers that join
// the cluster, and it stops replicating existing servers by causing the
// replicate go-routine to return.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.init()
	r.closed = true
	close(r.closeChan)
	return nil
}
