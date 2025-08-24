package server

import (
	api "Proglog/api/v1"
	"context"

	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(uint64 uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.ChainStreamInterceptor(
			grpcAuth.StreamServerInterceptor(authenticate),
		),
		grpc.ChainUnaryInterceptor(
			grpcAuth.UnaryServerInterceptor(authenticate),
		),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// Produce appends a record to the log. It's the implementation of the
// Produce RPC defined in the `api/v1/log.proto` file.
func (srv *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := srv.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}
	offset, err := srv.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume reads a record from the log. It's the implementation of the
// Consume RPC defined in the `api/v1/log.proto` file.
func (srv *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := srv.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}
	record, err := srv.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream handles bidirectional streaming for producing records. It continuously
// receives ProduceRequest messages from the client stream, appends each record to the
// log using the Produce method, and sends back ProduceResponse messages with offsets.
func (srv *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := srv.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream handles server-side streaming for consuming records. It continuously
// reads records from the log starting at the requested offset, sending each record
// to the client stream. It automatically increments the offset and handles context
// cancellation, continuing to poll when reaching the end of the log.
func (srv *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := srv.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// authenticate extracts the client's certificate information from the gRPC context
// and adds the subject (CommonName) to the context for authorization purposes.
// It returns an error if peer information cannot be retrieved from the context.
func authenticate(ctx context.Context) (context.Context, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}
	if pr.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := pr.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName

	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

type subjectContextKey struct{}

// subject extracts the authenticated client's identity from the context.
// It retrieves the CommonName from the client's TLS certificate that was
// previously stored by the authenticate function during request processing.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}
