package server

import (
	"context"
	"github.com/srivastavcodes/distributed-event-logger/internal/auth"
	"github.com/srivastavcodes/distributed-event-logger/internal/config"
	"github.com/srivastavcodes/distributed-event-logger/internal/log"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"testing"
)

func TestServer(t *testing.T) {
	collection := map[string]func(t *testing.T, rootClient, nobodyClient protolog.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	}
	for scenario, fn := range collection {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(cfg *Config)) (protolog.LogClient, protolog.LogClient, *Config, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err)

	newClient := func(certPath, keyPath string) (*grpc.ClientConn, protolog.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: certPath,
			KeyFile:  keyPath,
			CAFile:   "../../" + config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)

		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.NewClient(listener.Addr().String(), opts...)
		require.NoError(t, err)

		client := protolog.NewLogClient(conn)
		return conn, client, opts
	}
	rootConn, rootClient, _ := newClient(
		"../../"+config.RootClientCertFile,
		"../../"+config.RootClientKeyFile,
	)
	nobodyConn, nobodyClient, _ := newClient(
		"../../"+config.NobodyClientCertFile,
		"../../"+config.NobodyClientKeyFile,
	)
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      "../../" + config.ServerCertFile,
		KeyFile:       "../../" + config.ServerKeyFile,
		CAFile:        "../../" + config.CAFile,
		Server:        true,
		ServerAddress: listener.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("./", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	var (
		model  = "../../" + config.ACLModelFile
		policy = "../../" + config.ACLPolicyFile
	)
	authorizer := auth.NewAuthorizer(model, policy)
	cfg := &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)
	go func() {
		_ = server.Serve(listener)
	}()
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		clog.Remove()
		listener.Close()
	}
}

func testProduceConsume(t *testing.T, client, _ protolog.LogClient, _ *Config) {
	ctx := context.Background()

	want := &protolog.Record{
		Value: []byte("hello world"),
	}
	prodReq := &protolog.ProduceRequest{
		Record: want,
	}
	produce, err := client.Produce(ctx, prodReq)
	require.NoError(t, err)

	consReq := &protolog.ConsumeRequest{
		Offset: produce.Offset,
	}
	consume, err := client.Consume(ctx, consReq)
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ protolog.LogClient, config *Config) {
	ctx := context.Background()

	prodReq := &protolog.ProduceRequest{
		Record: &protolog.Record{
			Value: []byte("hello world"),
		},
	}
	produce, err := client.Produce(ctx, prodReq)
	require.NoError(t, err)

	consReq := &protolog.ConsumeRequest{
		Offset: produce.Offset + 1,
	}
	consume, err := client.Consume(ctx, consReq)
	if consume != nil {
		t.Fatal("consume not nil")
	}
	var errOffOutOfRange protolog.ErrOffsetOutOfRange
	var (
		got  = status.Code(err)
		want = status.Code(errOffOutOfRange.GRPCStatus().Err())
	)
	require.Equalf(t, got, want, "got err: %v, want: %v", got, want)
}

func testProduceConsumeStream(t *testing.T, client, _ protolog.LogClient, config *Config) {
	ctx := context.Background()

	records := []*protolog.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&protolog.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}

	}
	{
		stream, err := client.ConsumeStream(ctx, &protolog.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record,
				&protolog.Record{
					Value:  record.Value,
					Offset: uint64(i),
				})
		}
	}
}

func testUnauthorized(t *testing.T, _, client protolog.LogClient, config *Config) {
	prodReq := &protolog.ProduceRequest{
		Record: &protolog.Record{
			Value: []byte("hello world"),
		},
	}
	produce, err := client.Produce(context.Background(), prodReq)
	require.Nil(t, produce, "produce response should be nil")

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	require.Equal(t, gotCode, wantCode)

	consReq := &protolog.ConsumeRequest{
		Offset: 0,
	}
	consume, err := client.Consume(context.Background(), consReq)
	require.Nil(t, consume, "consume response should be nil")

	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	require.Equal(t, gotCode, wantCode)
}
