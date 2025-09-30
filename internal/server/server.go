package server

import (
	"context"
	"errors"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.ChainStreamInterceptor(
			grpc_auth.StreamServerInterceptor(authenticate),
		),
		grpc.ChainUnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
	)
	gsrv := grpc.NewServer(opts...)

	srv, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}
	protolog.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type CommitLog interface {
	Append(*protolog.Record) (uint64, error)
	Read(uint64) (*protolog.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

var _ protolog.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	*Config
	protolog.UnimplementedLogServer
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *protolog.ProduceRequest) (*protolog.ProduceResponse, error) {
	err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction)
	if err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &protolog.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *protolog.ConsumeRequest) (*protolog.ConsumeResponse, error) {
	err := s.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction)
	if err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &protolog.ConsumeResponse{Record: record}, nil
}

// ProduceStream implements a bidirectional streaming rpc so the client
// can stream data into the server's log and the server can tell the
// client whether each request succeeded.
func (s *grpcServer) ProduceStream(stream protolog.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream implements a server-side streaming rpc so the client can
// tell the server where in the logs to read records, and then the server
// will stream every record that follows - even record's that aren't in
// the log yet.
//
// When the server reaches the end of the log, the server will wait until
// someone appends a record to the log and then continue streaming records
// to the client.
func (s *grpcServer) ConsumeStream(req *protolog.ConsumeRequest, stream protolog.Log_ConsumeStreamServer) error {
	var errOffsetOutOfRange protolog.ErrOffsetOutOfRange
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		res, err := s.Consume(stream.Context(), req)
		if errors.Is(err, errOffsetOutOfRange) {
			continue
		}
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
		req.Offset++
	}
}

// authenticate is an interceptor that reads the subject out of the client's
// cert and writes it to the RPCs context.
func authenticate(ctx context.Context) (context.Context, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer",
		).Err()
	}
	if pr.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := pr.AuthInfo.(credentials.TLSInfo)

	sub := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, sub)
	return ctx, nil
}

type subjectContextKey struct{}

// subject returns the client's certs subject so we can identify a client
// and check their access.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}
