package dlog

import (
	"fmt"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
)

const (
	// offWidth is how much space storing the offset would take
	// stored as uint32s (4 bytes).
	offWidth uint64 = 4

	// posWidth is how much space storing the position would take
	// stored as uint64s (8 bytes).
	posWidth uint64 = 8

	// entWidth is the total width of an index entry.
	entWidth = offWidth + posWidth
)

// index entries contains the record's offset and its position in
// the store file.
type index struct {
	// file is the persisted File of index.
	file *os.File

	// mmap is the memory mapped file.
	mmap mmap.MMap

	// size tells us the size of the index and where to write
	// the next entry appended to the index.
	size uint64
}

// newIndex creates an index for the given file. It creates the index and saves
// the current size of the file to track the amount of data being added.
// We grow the file to the MaxIndexBytes before memory mapping the file.
func newIndex(file *os.File, config Config) (*index, error) {
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, fmt.Errorf("newIndex failed to stat file: %w", err)
	}
	idx := &index{file: file}
	idx.size = uint64(fi.Size())

	// increase file size before memory mapping. Adds empty space at the EOF.
	err = os.Truncate(file.Name(), int64(config.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}
	idx.mmap, err = mmap.Map(idx.file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Write appends the given entry(off) and position to the index. It validates that
// there's space then appends the (off) and (pos) to memory-mapped file after
// encoding.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// |existing data:--offset(4bytes)--|
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// |existing data|--offset(4bytes)--:--position(8bytes)--|
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += entWidth
	return nil
}

// Name returns the index's file path.
func (i *index) Name() string {
	return i.file.Name()
}

// Read takes in an entry and returns the associated record's position in the
// store. The given offset is relative to the segment's base offset; 0 is
// always the offset of the index's first entry, 1 is the second entry and so on.
// -1 gives you the last entry in the index.
func (i *index) Read(off int64) (ent uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if off == -1 {
		ent = uint32((i.size / entWidth) - 1)
	} else {
		ent = uint32(off)
	}
	offset := uint64(ent) * entWidth
	if i.size < offset+entWidth {
		return 0, 0, io.EOF
	}
	ent = enc.Uint32(i.mmap[offset : offset+offWidth])
	pos = enc.Uint64(i.mmap[offset+offWidth : offset+entWidth])
	return ent, pos, nil
}

// Close flushes the contents of the mmap and the in-memory file to the
// stable storage and truncates the file to the index's actual size.
// The file is closed.
func (i *index) Close() error {
	if err := i.mmap.Flush(); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	err := i.file.Truncate(int64(i.size))
	if err != nil {
		return err
	}
	return i.file.Close()
}
