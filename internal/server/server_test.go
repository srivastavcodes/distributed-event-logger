package server

import (
	api "Proglog/api/v1"
	"Proglog/internal/auth"
	"Proglog/internal/config"
	"Proglog/internal/log"
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T,
		nobodyClient api.LogClient,
		rootClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log": testProduceConsume,
		"produce/consume stream":                    testProduceConsumeStream,
		"consume past log boundary":                 testConsumePastBoundary,
		"unauthorized":                              testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, nobodyClient, rootClient, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient, nobodyClient api.LogClient, cfg *Config,
	teardown func(),
) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		clientCred := credentials.NewTLS(clientTLSConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCred)}

		cc, err := grpc.NewClient(listener.Addr().String(), opts...)
		require.NoError(t, err)

		client := api.NewLogClient(cc)
		return cc, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:   config.ServerCertFile,
		CAFile:     config.CAFile,
		KeyFile:    config.ServerKeyFile,
		Server:     true,
		ServerAddr: listener.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer, err := auth.NewAuthorizer(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)

	cfg = &Config{
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
		nobodyConn.Close()
		rootConn.Close()
		listener.Close()
	}
}

func testProduceConsume(t *testing.T, _, client api.LogClient, _ *Config) {
	ctx := context.Background()

	want := &api.Record{Values: []byte("hello world")}
	preq := &api.ProduceRequest{Record: want}

	produce, err := client.Produce(ctx, preq)
	require.NoError(t, err)

	creq := &api.ConsumeRequest{Offset: produce.Offset}
	consume, err := client.Consume(ctx, creq)
	require.NoError(t, err)

	require.Equal(t, want.Values, consume.Record.Values)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, _, client api.LogClient, _ *Config) {
	ctx := context.Background()

	preq := &api.ProduceRequest{
		Record: &api.Record{
			Values: []byte("hello world"),
		},
	}
	produce, err := client.Produce(ctx, preq)
	require.NoError(t, err)

	creq := &api.ConsumeRequest{Offset: produce.Offset + 1}
	consume, err := client.Consume(ctx, creq)
	if consume != nil {
		t.Fatal("consume not nil")
	}
	var grpcErr api.ErrOffsetOutOfRange

	got := status.Code(err)
	want := status.Code(grpcErr.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, _, client api.LogClient, _ *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Values: []byte("first message"),
		Offset: 0,
	}, {
		Values: []byte("second message"),
		Offset: 1,
	}}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			preq := &api.ProduceRequest{Record: record}
			err = stream.Send(preq)
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}

	}
	{
		creq := &api.ConsumeRequest{Offset: 0}
		stream, err := client.ConsumeStream(ctx, creq)
		require.NoError(t, err)

		for i, record := range records {
			rec := &api.Record{
				Values: record.Values,
				Offset: uint64(i),
			}
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, rec)
		}
	}
}

func testUnauthorized(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()
	preq := &api.ProduceRequest{
		Record: &api.Record{
			Values: []byte("hello world"),
		},
	}
	produce, err := client.Produce(ctx, preq)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	creq := &api.ConsumeRequest{Offset: 0}

	consume, err := client.Consume(ctx, creq)
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
