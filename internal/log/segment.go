package log

import (
	api "Proglog/api/v1"
	"fmt"
	"os"
	"path"

	"github.com/golang/protobuf/proto"
)

type segment struct {
	store  *store
	index  *index
	config Config

	baseOffset, nextOffset uint64
}

// newSegment creates a new log segment with store and index files.
//
// Files are created as {baseOffset}.store and {baseOffset}.index in the directory.
// It reads the last index entry to determine the next offset for new records.
func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	seg := &segment{
		baseOffset: baseOffset,
		config:     config,
	}
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		return nil, err
	}
	if seg.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE, 0644,
	)
	if err != nil {
		return nil, err
	}
	if seg.index, err = newIndex(indexFile, config); err != nil {
		return nil, err
	}
	if off, _, err := seg.index.Read(-1); err != nil {
		seg.nextOffset = baseOffset
	} else {
		seg.nextOffset = baseOffset + uint64(off) + 1
	}
	return seg, nil
}

// Append stores a new record in the segment and assigns it the next available offset.
// It serializes the record to protobuf format and writes it to the store file.
// The record's position is indexed using its relative offset for fast retrieval.
// This allows the log to maintain sequential ordering while enabling efficient lookups.
func (seg *segment) Append(record *api.Record) (offset uint64, err error) {
	curr := seg.nextOffset
	record.Offset = curr

	data, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := seg.store.Append(data)
	if err != nil {
		return 0, err
	}
	err = seg.index.Write(uint32(seg.nextOffset-seg.baseOffset), pos)
	if err != nil {
		return 0, err
	}
	seg.nextOffset++
	return curr, nil
}

// Read retrieves a record from the segment by its offset.
//
// It looks up the record's position in the index using the relative offset, reads
// the data from the store file, and deserializes it back into a Record.
// This enables efficient random access to any record in the segment.
func (seg *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := seg.index.Read(int64(off - seg.baseOffset))
	if err != nil {
		return nil, err
	}
	data, err := seg.store.Read(pos)
	if err != nil {
		return nil, err
	}
	var record *api.Record
	err = proto.Unmarshal(data, record)
	return record, err
}

func (seg *segment) IsMaxed() bool {
	return seg.store.size >= seg.config.Segment.MaxStoreBytes ||
		seg.index.size >= seg.config.Segment.MaxIndexBytes
}

func (seg *segment) Close() error {
	if err := seg.index.Close(); err != nil {
		return err
	}
	if err := seg.store.Close(); err != nil {
		return err
	}
	return nil
}

func (seg *segment) Remove() error {
	if err := seg.Close(); err != nil {
		return err
	}
	if err := os.Remove(seg.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(seg.store.Name()); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of n2 in n1,
// for example, nearestMultiple(9, 4) == 8.
//
// We take the lesser multiple to make sure we stay under the user's disk capacity.
func nearestMultiple(n1, n2 uint64) uint64 {
	if n1 >= 0 {
		return (n1 / n2) * n2
	}
	return ((n1 - n2 + 1) / n2) * n2
}
