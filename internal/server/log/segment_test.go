package log

import (
	"io"
	"os"
	"testing"

	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment_test")
	defer os.RemoveAll(dir)

	var config Config
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, config)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	want := &protolog.Record{Value: []byte("hello world")}
	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}
	_, err = s.Append(want)
	require.Equal(t, err, io.EOF)

	// maxed Index
	require.True(t, s.IsMaxed())

	config.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	config.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, config)
	require.NoError(t, err)

	// maxed Store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
