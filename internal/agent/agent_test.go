package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/srivastavcodes/distributed-event-logger/internal/config"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      "../../" + config.ServerCertFile,
		KeyFile:       "../../" + config.ServerKeyFile,
		CAFile:        "../../" + config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      "../../" + config.ClientCertFile,
		KeyFile:       "../../" + config.ClientKeyFile,
		CAFile:        "../../" + config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*Agent
	for i := 0; i < 3; i++ {
		var (
			ports    = dynaport.Get(2)
			bindAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
			rpcPort  = ports[1]
		)
		dataDir, err := os.MkdirTemp("./", "agent-test-dlog")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}
		agent, err := NewAgent(Config{
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			DataDir:         dataDir,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			NodeName:        fmt.Sprintf("%d", i),
			Bootstrap:       i == 0,
			StartJoinAddrs:  startJoinAddrs,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)

	// leader produce and consume
	leaderClient := client(t, agents[0], peerTLSConfig)
	prodReq := &protolog.ProduceRequest{
		Record: &protolog.Record{
			Value: []byte("foo"),
		},
	}
	prodRes, err := leaderClient.Produce(context.Background(), prodReq)
	require.NoError(t, err)

	consReq := &protolog.ConsumeRequest{
		Offset: prodRes.Offset,
	}
	consRes, err := leaderClient.Consume(context.Background(), consReq)

	require.NoError(t, err)
	require.Equal(t, consRes.Record.Value, []byte("foo"))

	time.Sleep(3 * time.Second)

	// follower has the same value?
	followerClient := client(t, agents[1], peerTLSConfig)
	consReq = &protolog.ConsumeRequest{
		Offset: prodRes.Offset,
	}
	consRes, err = followerClient.Consume(context.Background(), consReq)

	require.NoError(t, err)
	require.Equal(t, consRes.Record.Value, []byte("foo"))

	consRes, err = leaderClient.Consume(context.Background(), &protolog.ConsumeRequest{
		Offset: prodRes.Offset + 1,
	})
	require.Nil(t, consRes)
	require.Error(t, err)

	got := status.Code(err)
	want := status.Code(protolog.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func client(t *testing.T, agent *Agent, tlsConfig *tls.Config) protolog.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(tlsCreds))

	rpcAddr, err := agent.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.NewClient(rpcAddr, opts...)
	require.NoError(t, err)

	return protolog.NewLogClient(conn)
}
