package log

import (
	api "Proglog/api/v1"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Values: []byte("hello world")}

	var config Config
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entWidth * 3

	seg, err := newSegment(dir, 16, config)
	require.NoError(t, err)
	require.Equal(t, uint64(16), seg.nextOffset, seg.nextOffset)
	require.False(t, seg.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := seg.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Values, got.Values)
	}

	_, err = seg.Append(want)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, seg.IsMaxed())

	config.Segment.MaxStoreBytes = uint64(len(want.Values) * 3)
	config.Segment.MaxIndexBytes = 1024

	seg, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	// maxed store
	require.True(t, seg.IsMaxed())

	err = seg.Remove()
	require.NoError(t, err)

	seg, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	require.False(t, seg.IsMaxed())
}
