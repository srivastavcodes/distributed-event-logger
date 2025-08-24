package log

import (
	"errors"
	"io"
	"os"
	api "proglog/api/v1"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			var config Config
			config.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, config)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testTruncate(t *testing.T, log *Log) {
	write := &api.Record{
		Values: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(write)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}

func testReader(t *testing.T, log *Log) {
	write := &api.Record{
		Values: []byte("hello world"),
	}
	off, err := log.Append(write)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	var read api.Record
	err = proto.Unmarshal(b[lenWidth:], &read)
	require.NoError(t, err)
	require.Equal(t, write.Values, read.Values)
}

func testInitExisting(t *testing.T, log *Log) {
	write := &api.Record{
		Values: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(write)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	l, err := NewLog(log.Directory, log.Config)
	require.NoError(t, err)

	off, err = l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	var apiErr api.ErrOffsetOutOfRange
	require.True(t, errors.As(err, &apiErr))
}

func testAppendRead(t *testing.T, log *Log) {
	write := &api.Record{
		Values: []byte("hello world"),
	}
	off, err := log.Append(write)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, write.Values, read.Values)
}
