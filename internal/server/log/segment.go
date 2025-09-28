package log

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"google.golang.org/protobuf/proto"
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
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
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
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
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

// Append writes the record to the segment and returns the newly appended
// record's offset. The log returns the offset to the API response.
func (s *segment) Append(record *protolog.Record) (off uint64, err error) {
	var cur = s.nextOffset
	record.Offset = cur

	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(bytes)
	if err != nil {
		return 0, err
	}
	// index offsets are relative to the base offset.
	err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read returns the record for the given offset.
func (s *segment) Read(off uint64) (*protolog.Record, error) {
	// index offsets are relative to the base offset.
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	bytes, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &protolog.Record{}

	err = proto.Unmarshal(bytes, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size, either by writing
// too much to the store or the index.
//
// If you wrote a small number of long logs, you'd hit the StoreBytes limit; if you
// wrote a large number of small logs, you'd hit the IndexByte limit. The log uses
// this method to know it needs to create a new segment.
func (s *segment) IsMaxed() bool {
	var (
		storeMax = s.store.size >= s.config.Segment.MaxStoreBytes
		indexMax = s.index.size >= s.config.Segment.MaxIndexBytes
	)
	return storeMax || indexMax
}

// Remove closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	var err error

	err = os.Remove(s.index.Name())
	if err != nil {
		return err
	}
	err = os.Remove(s.store.Name())
	return err
}

// Close closes the index and the store for this segment.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of (k) in (j).
// For eg: nearestMultiple(9, 4) == 8. We take the lesser multiple to
// make sure we stay under the user's disk capacity.
func nearestMultiple(j, k uint64) uint64 { return (j / k) * k }
