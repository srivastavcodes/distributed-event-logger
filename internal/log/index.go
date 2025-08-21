package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	size uint64
	mmap gommap.MMap
}

func (idx *index) Name() string {
	return idx.file.Name()
}

// newIndex creates a new index backed by a memory-mapped file. It truncates the
// file to the configured max size and maps it into memory for efficient random
// access to offset/position pairs.
func newIndex(f *os.File, config Config) (*index, error) {
	idx := &index{file: f}

	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(file.Size())
	if err = os.Truncate(f.Name(), int64(config.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// Map the entire f into virtual memory for direct byte-level access. MAP_SHARED
	// makes changes visible to all processes mapping the same f.
	// Changes to the mapped memory are automatically synced to disk by the OS.
	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Read retrieves an index entry at the given position. If (in) is -1, returns the last
// entry in the index.
//
// Returns the offset (out), position (pos), and any error.
//
// Returns io.EOF if the index is empty or the requested entry doesn't exist.
func (idx *index) Read(in int64) (out uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((idx.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	// Convert entry index to byte offset in the memory-mapped file
	pos = uint64(out) * entWidth
	if idx.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(idx.mmap[pos : pos+offWidth])
	pos = enc.Uint64(idx.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends a new index entry with the given offset and position.
//
// Returns io.EOF for insufficient space in the memory-mapped file.
func (idx *index) Write(off uint32, pos uint64) error {
	if uint64(len(idx.mmap)) < idx.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(idx.mmap[idx.size:idx.size+offWidth], off)
	enc.PutUint64(idx.mmap[idx.size+offWidth:idx.size+entWidth], pos)
	idx.size += entWidth
	return nil
}

// Close makes sure the memory-mapped file has synced its data to the persisted file. And
// the persisted file has flushed its data to a stable storage. Then it truncates the file
// to the amount of data that's actually in it and closes the file.
func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.file.Close()
}
