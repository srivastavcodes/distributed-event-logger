package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	log2 "github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"github.com/srivastavcodes/distributed-event-logger/internal/discovery"
	"github.com/srivastavcodes/distributed-event-logger/internal/log"
	"github.com/srivastavcodes/distributed-event-logger/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent runs on every service instance, setting up and connecting all the
// different components. The struct references each component (dlog, server,
// membership, replicator) that the Agent manages.
type Agent struct {
	Config

	mux        cmux.CMux
	dlog       *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdownChan chan struct{}
	shutdownLock sync.Mutex
}

// Config comprises the Agent's component parameters.
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	Bootstrap       bool
	LogConfig       log.Config
	StartJoinAddrs  []string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// NewAgent creates an Agent and runs a set of methods to set up and run
// the agent's components. After running NewAgent it is expected to have
// a running, functioning service.
func NewAgent(config Config) (*Agent, error) {
	agent := &Agent{
		Config:       config,
		shutdownChan: make(chan struct{}),
	}
	setup := []func() error{
		agent.setupLogger,
		agent.setupMux,
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go agent.serve()
	return agent, nil
}

// setupMux creates a listener on our rpc address that'll accept both raft
// and grpc connections and then creates the mux with the listener.
// The mux will accept connections on that listener and match connections
// based on the configured rules.
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)

	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(listener)
	return nil
}

// setupLogger configures the global logger to write debug messages
// to stderr with colored console output for easy reading.
func (a *Agent) setupLogger() error {
	log2.Logger = log2.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	return nil
}

// setupLog creates the distributed dlog that stores and manages all
// the records on this node using the configured data directory.
func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		connType := []byte{byte(log.RaftRPC)}
		return bytes.Compare(b, connType) == 0
	})
	var logConfig log.Config

	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.ServerTLSConfig,
		a.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	var err error
	a.dlog, err = log.NewDistributedLog(a.Config.DataDir, logConfig)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.dlog.WaitForLeader(3 * time.Second)
	}
	return err
}

// setupServer creates and starts the gRPC server that handles client requests.
// It configures TLS if provided, binds to the RPC address, and starts serving
// requests in a background goroutine.
func (a *Agent) setupServer() error {
	serverConfig := &server.Config{
		CommitLog: a.dlog,
	}
	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error

	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	// Because we've multiplexed two connection types (raft and grpc) and we
	// added a matcher for the raft connections, we know all other conns must
	// be grpc connections. cmux.Any() matches any connection.
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		// We tell our grpc server to serve on the multiplexed listener.
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

// setupMembership sets up node discovery along with dlog replication.
//
// It creates a replicator to sync logs between nodes and configures
// membership discovery so nodes can find each other in the cluster.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.NewMembership(a.dlog, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// Shutdown ensures the agent will shut down once even if called multiple
// times. After call to Shutdown() agent and it's components shut down.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.shutdown = true

	close(a.shutdownChan)
	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.dlog.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// server starts multiplexing the listener and is blocking;
// should be called in a goroutine.
func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
