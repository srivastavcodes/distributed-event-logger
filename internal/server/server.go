package server

import (
	"context"
	"errors"
	"google.golang.org/grpc"

	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
)

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	var (
		gsrv     = grpc.NewServer()
		srv, err = newGrpcServer(config)
	)
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

type Config struct {
	CommitLog CommitLog
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

// TODO: add context handling after project completion

func (s *grpcServer) Produce(_ context.Context, req *protolog.ProduceRequest) (*protolog.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &protolog.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(_ context.Context, req *protolog.ConsumeRequest) (*protolog.ConsumeResponse, error) {
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
