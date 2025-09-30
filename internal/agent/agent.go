package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/rs/zerolog"
	log2 "github.com/rs/zerolog/log"
	"github.com/srivastavcodes/distributed-event-logger/internal/discovery"
	"github.com/srivastavcodes/distributed-event-logger/internal/log"
	"github.com/srivastavcodes/distributed-event-logger/internal/server"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent runs on every service instance, setting up and connecting all the
// different components. The struct references each component (log, server,
// membership, replicator) that the Agent manages.
type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

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
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return agent, nil
}

// setupLogger configures the global logger to write debug messages
// to stderr with colored console output for easy reading.
func (a *Agent) setupLogger() error {
	log2.Logger = log2.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	return nil
}

// setupLog creates the distributed log that stores and manages
// all the records on this node using the configured data directory.
func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.Config.DataDir, a.LogConfig)
	return err
}

// setupServer creates and starts the gRPC server that handles client requests.
// It configures TLS if provided, binds to the RPC address, and starts serving
// requests in a background goroutine.
func (a *Agent) setupServer() error {
	serverConfig := &server.Config{
		CommitLog: a.log,
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
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	listen, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(listen); err != nil {
			_ = a.Shutdown()
		}
	}()
	return nil
}

// setupMembership sets up node discovery along with log replication.
//
// It creates a replicator to sync logs between nodes and configures
// membership discovery so nodes can find each other in the cluster.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	var opts []grpc.DialOption

	if a.PeerTLSConfig != nil {
		creds := credentials.NewTLS(a.PeerTLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	conn, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := protolog.NewLogClient(conn)

	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	discfg := discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.StartJoinAddrs,
	}
	a.membership, err = discovery.NewMembership(a.replicator, discfg)
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
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
