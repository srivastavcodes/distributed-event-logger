package server

import (
	api "Proglog/api/v1"
	"context"

	"google.golang.org/grpc"
)

var _ api.LogServer = (*grpcServer)(nil)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(uint65 uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

func NewGrpcServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
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
func (srv *grpcServer) Produce(_ context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := srv.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume reads a record from the log. It's the implementation of the
// Consume RPC defined in the `api/v1/log.proto` file.
func (srv *grpcServer) Consume(_ context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
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
