package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	api "proglog/api/v1"
	"proglog/internal/auth"
	"proglog/internal/discovery"
	"proglog/internal/log"
	"proglog/internal/server"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

func (config Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(config.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, config.RPCPort), nil
}

type Agent struct {
	Config     Config
	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func NewAgent(config Config) (*Agent, error) {
	ag := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		ag.setupLogger,
		ag.setupLog,
		ag.setupServer,
		ag.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return ag, nil
}

func (ag *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (ag *Agent) setupLog() error {
	var err error
	ag.log, err = log.NewLog(ag.Config.DataDir, log.Config{})
	return err
}

func (ag *Agent) setupServer() error {
	authorizer, err := auth.NewAuthorizer(ag.Config.ACLModelFile, ag.Config.ACLPolicyFile)
	if err != nil {
		return err
	}
	serverConfig := &server.Config{
		CommitLog:  ag.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption

	if ag.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(ag.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	ag.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	rpcAddr, err := ag.Config.RPCAddr()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := ag.server.Serve(listener); err != nil {
			_ = ag.Shutdown()
		}
	}()
	return err
}

func (ag *Agent) setupMembership() error {
	rpcAddr, err := ag.Config.RPCAddr()
	if err != nil {
		return err
	}
	var opts []grpc.DialOption

	if ag.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(ag.Config.PeerTLSConfig),
		))
	}
	conn, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	ag.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	ag.membership, err = discovery.NewMembership(ag.replicator, discovery.Config{
		NodeName: ag.Config.NodeName,
		BindAddr: ag.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: ag.Config.StartJoinAddrs,
	})
	return err
}

func (ag *Agent) Shutdown() error {
	ag.shutdownLock.Lock()
	defer ag.shutdownLock.Unlock()

	if ag.shutdown {
		return nil
	}
	ag.shutdown = true
	close(ag.shutdowns)

	shutdown := []func() error{
		ag.membership.Leave,
		ag.replicator.Close,
		func() error {
			ag.server.GracefulStop()
			return nil
		},
		ag.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
