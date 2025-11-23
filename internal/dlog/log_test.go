package dlog

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	collection := map[string]func(t *testing.T, log *Log){
		"append and read a record":    testAppendRead,
		"offset out of range error":   testOutOfRangeError,
		"init with existing segments": testInitExisting,
		"reader":                      testReader,
		"truncate":                    testTruncate,
	}
	for scenario, fn := range collection {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("./", "store-test")
			require.NoError(t, err)
			defer func() { _ = os.RemoveAll(dir) }()

			config := Config{}
			config.Segment.MaxStoreBytes = 32

			log, err := NewLog(dir, config)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	writeRecord := &protolog.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(writeRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	readRecord, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, writeRecord.Value, readRecord.Value)
}

func testOutOfRangeError(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)

	var protoErr protolog.ErrOffsetOutOfRange
	errors.As(err, &protoErr)

	require.Equal(t, uint64(1), protoErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	writeRecord := &protolog.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(writeRecord)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	log, err = NewLog(log.Directory, log.Config)
	require.NoError(t, err)

	off, err = log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	writeRecord := &protolog.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(writeRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()

	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	readRecord := &protolog.Record{}
	err = proto.Unmarshal(bytes[lenWidth:], readRecord)
	require.NoError(t, err)
	require.Equal(t, writeRecord.Value, readRecord.Value)
}

func testTruncate(t *testing.T, log *Log) {
	writeRecord := &protolog.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(writeRecord)
		require.NoError(t, err)
	}
	err := log.TruncatePrev(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
