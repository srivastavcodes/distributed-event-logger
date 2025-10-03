package loadbalancer

import (
	"net"
	"net/url"
	"testing"

	"github.com/srivastavcodes/distributed-event-logger/internal/config"
	"github.com/srivastavcodes/distributed-event-logger/internal/server"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestResolver(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      "../../" + config.ServerCertFile,
		KeyFile:       "../../" + config.ServerKeyFile,
		CAFile:        "../../" + config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var (
		serverCreds = credentials.NewTLS(serverTLSConfig)
		srvConfig   = &server.Config{GetServerer: &getServers{}}
	)
	srv, err := server.NewGRPCServer(srvConfig, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go srv.Serve(listener)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      "../../" + config.ClientCertFile,
		KeyFile:       "../../" + config.ClientKeyFile,
		CAFile:        "../../" + config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(peerTLSConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}
	res := &Resolver{}
	target := resolver.Target{URL: url.URL{
		Scheme: Name,
		Host:   listener.Addr().String(),
	}}
	conn := &clientConn{}
	_, err = res.Build(target, conn, opts)
	require.NoError(t, err)

	wantState := resolver.State{
		Addresses: []resolver.Address{
			{
				Addr:       "localhost:8001",
				Attributes: attributes.New("is_leader", true),
			},
			{
				Addr:       "localhost:8002",
				Attributes: attributes.New("is_leader", false),
			},
		},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	res.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

type getServers struct{}

func (g *getServers) GetServers() ([]*protolog.Server, error) {
	return []*protolog.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:8001",
			IsLeader: true,
		},
		{
			Id:      "follower",
			RpcAddr: "localhost:8002",
		},
	}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(_ error) {}

func (c *clientConn) NewAddress(_ []resolver.Address) {}

func (c *clientConn) NewServiceConfig(_ string) {}

func (c *clientConn) ParseServiceConfig(_ string) *serviceconfig.ParseResult { return nil }
