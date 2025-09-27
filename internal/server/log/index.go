package log

import (
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
	entWidth uint64 = offWidth + posWidth
)

// index entries contains the record's offset and its position in
// the store file.
type index struct {
	// File is the persisted file of index.
	*os.File

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
	idx := &index{File: file}

	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	err = os.Truncate(file.Name(), int64(config.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}
	idx.mmap, err = mmap.Map(idx.File, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Close flushes the contents of the mmap and the in-memory file to the
// stable storage and truncates the file to the index's actual size.
// The file is closed.
func (i *index) Close() error {
	if err := i.mmap.Flush(); err != nil {
		return err
	}
	if err := i.File.Sync(); err != nil {
		return err
	}
	err := i.File.Truncate(int64(i.size))
	if err != nil {
		return err
	}
	return i.File.Close()
}
