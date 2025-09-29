package server

import (
	"context"
	"github.com/srivastavcodes/distributed-event-logger/internal/log"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"testing"
)

func TestServer(t *testing.T) {
	collection := map[string]func(t *testing.T, logClient protolog.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	}
	for scenario, fn := range collection {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(config *Config)) (protolog.LogClient, *Config, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.NewClient(listener.Addr().String(), clientOpts...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("./", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	config := &Config{CommitLog: clog}
	if fn != nil {
		fn(config)
	}
	server, err := NewGRPCServer(config)
	require.NoError(t, err)
	go func() {
		_ = server.Serve(listener)
	}()
	client := protolog.NewLogClient(conn)
	return client, config, func() {
		server.Stop()
		conn.Close()
		listener.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client protolog.LogClient, _ *Config) {
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

func testConsumePastBoundary(t *testing.T, client protolog.LogClient, config *Config) {
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

func testProduceConsumeStream(t *testing.T, client protolog.LogClient, config *Config) {
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
