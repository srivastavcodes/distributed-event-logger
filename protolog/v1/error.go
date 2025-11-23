package protolog

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	var (
		sts = status.New(codes.NotFound, fmt.Sprintf("offset out of range: %d", e.Offset))
		msg = fmt.Sprintf("The requested offset is outside the dlog's range: %d", e.Offset)
	)
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

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
