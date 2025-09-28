package log

import (
	"fmt"
	"os"
	"path/filepath"
)

// segment wraps the index and store types to co-ordinate operations
// across the two.
//
// When log appends the record to the active segment,
// the segment needs to write the data to its store and add a new
// entry in the index.
//
// Similarly for reads, the segment needs to look
// up the entry from the index and then fetch data from the store.
type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

// newSegment is called when the log needs to add a new segment, such as when
// the current active segment hits its max size. Index and Store files are
// created if they don't exist yet with os.O_APPEND flag to make the OS append
// to it when writing.
func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	s := &segment{
		config:     config,
		baseOffset: baseOffset,
	}
	storeFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(filepath.Join(fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, config)
	if err != nil {
		return nil, err
	}

	off, _, err := s.index.Read(-1)
	if err != nil {
		// if the index is empty, then the next record appended to the segment
		// would be the first record and its offset would be segment's base
		// offset.
		s.nextOffset = baseOffset
	} else {
		// if the index has at-least one entry, then that means the offset of
		// the next record written should take the offset at the end of the
		// segment, which we get by adding 1 to the base offset and relative
		// offset.
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}
