package server

import (
	"errors"
	"sync"
)

var (
	ErrOffsetNotFound = errors.New("offset not found")
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (cl *Log) Append(record Record) (uint64, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	record.Offset = uint64(len(cl.records))
	cl.records = append(cl.records, record)

	return record.Offset, nil
}

func (cl *Log) Read(offset uint64) (Record, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if offset >= uint64(len(cl.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return cl.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
