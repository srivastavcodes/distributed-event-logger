package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File

	mu   sync.Mutex
	size uint64
	buf  *bufio.Writer
}

func newStore(f *os.File) (*store, error) {
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(file.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes data to the store and returns the number of bytes written and position.
// The data is prefixed with its length (8 bytes) for reading back later.
//
// Returns: bytes written (including length prefix), starting position, and any error.
func (st *store) Append(data []byte) (n uint64, pos uint64, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	pos = st.size
	if err := binary.Write(st.buf, enc, uint64(len(data))); err != nil {
		return 0, 0, err
	}
	wd, err := st.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}
	wd += lenWidth
	st.size += uint64(wd)
	return uint64(wd), pos, nil
}

// Read retrieves data from the store at the given position.
// First reads the 8-byte length prefix, then reads that many data bytes.
//
// Returns the data and any error.
func (st *store) Read(pos uint64) ([]byte, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := st.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := st.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads len(p) bytes into p starting at offset off.
// Flushes the buffer first to ensure data consistency.
//
// Returns number of bytes read and any error.
func (st *store) ReadAt(p []byte, off int64) (int, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.buf.Flush(); err != nil {
		return 0, err
	}
	return st.File.ReadAt(p, off)
}

func (st *store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	err := st.buf.Flush()
	if err != nil {
		return err
	}
	return st.File.Close()
}
