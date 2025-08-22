package v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (eor ErrOffsetOutOfRange) GrpcStatus() *status.Status {
	sts := status.New(
		404,
		fmt.Sprintf("offset out of range: %d", eor.Offset),
	)
	msg := fmt.Sprintf("The requested offset is outside the log's range %d", eor.Offset)

	details := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := sts.WithDetails(details)
	if err != nil {
		return sts
	}
	return std
}

func (eor ErrOffsetOutOfRange) Error() string {
	if grpcErr := eor.GrpcStatus().Err(); grpcErr != nil {
		return grpcErr.Error()
	}
	return fmt.Sprintf("offset out of range: %d", eor.Offset)
}
