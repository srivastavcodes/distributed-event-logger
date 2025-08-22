package server

import (
	api "Proglog/api/v1"
	"Proglog/internal/log"
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, config *Config, teardown func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cc, err := grpc.NewClient(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	config = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(config)
	}
	server, err := NewGrpcServer(config)
	require.NoError(t, err)

	go func() {
		_ = server.Serve(listener)
	}()

	client = api.NewLogClient(cc)

	return client, config, func() {
		server.Stop()
		cc.Close()
		listener.Close()
		_ = clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, _ *Config) {
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

func testConsumePastBoundary(t *testing.T, client api.LogClient, _ *Config) {
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

func testProduceConsumeStream(t *testing.T, client api.LogClient, _ *Config) {
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
