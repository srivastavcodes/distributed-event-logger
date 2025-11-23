package dlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// lenWidth defines the number of bytes used to persist the record's length.
const lenWidth = 8

// enc defines the encoding that we persist record sizes and index entries in.
var enc = binary.BigEndian

// store is a wrapper around a file with two APIs to append and read
// bytes - to and from the file.
type store struct {
	file *os.File
	mu   sync.Mutex

	// size is the length of the file in bytes.
	size uint64
	buf  *bufio.Writer
}

// newStore creates a store for a given file. store.size is retrieved from
// file's existing size.
func newStore(file *os.File) (*store, error) {
	f, err := os.Stat(file.Name())
	if err != nil {
		return nil, fmt.Errorf("newStore failed to stat file: %w", err)
	}
	size := uint64(f.Size())

	return &store{
		file: file,
		size: size,
		buf:  bufio.NewWriter(file),
	}, nil
}

// Append persists the given bytes to the store. We write the length of the record
// so that, when we read the record, we know how many bytes to read.
// Writes to buffer instead of file directly to reduce system calls and improve
// performance, helps with frequent small appends.
//
// Returns byte written, and the position (pos) where the store holds the record in
// its file.
func (s *store) Append(bytes []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	if err = binary.Write(s.buf, enc, uint64(len(bytes))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(bytes)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Name returns the store's file path.
func (s *store) Name() string {
	return s.file.Name()
}

// Read returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// makes sure all the data is written before reading
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)

	// reads 8 bits[pos + 8] to get the size of the record at offset
	_, err := s.file.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))

	// reads after the offset+lenWidth(big endian representation of the size of the record).
	_, err = s.file.ReadAt(b, int64(pos+lenWidth))
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads len(p) bytes into (p) beginning at the (off) offset in the
// store's file. It implements io.ReaderAt on the store type.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// makes sure all the data is written before reading
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.file.ReadAt(p, off)
}

// Close persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}
