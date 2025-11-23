package log

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
)

// Log consists of a list of segments and a pointer to the active
// segment to append writes to.
type Log struct {
	mu sync.RWMutex

	// Directory is where segments are stored.
	Directory string
	Config    Config

	curSegment *segment
	segments   []*segment
}

// NewLog creates a Log instance with default config values if not specified
// and sets up that Log instance.
func NewLog(dir string, config Config) (*Log, error) {
	if config.Segment.MaxStoreBytes == 0 {
		config.Segment.MaxStoreBytes = 1024
	}
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = 1024
	}
	log := &Log{
		Directory: dir,
		Config:    config,
	}
	return log, log.setup()
}

// setup makes sure - the log is self-sufficient for setting itself up
// for the segments that already exists on disk, and if the log has no
// segments, it can bootstrap the initial segment.
func (l *Log) setup() error {
	entries, err := os.ReadDir(l.Directory)
	if err != nil {
		return err
	}
	var baseOffsets []uint64

	for _, entry := range entries {
		var (
			ext    = filepath.Ext(entry.Name())
			strOff = strings.TrimSuffix(entry.Name(), ext)
		)
		offset, err := strconv.ParseUint(strOff, 0, 0)
		if err != nil {
			return fmt.Errorf("invalid offset %s: %s", strOff, err)
		}
		baseOffsets = append(baseOffsets, offset)
	}
	slices.SortFunc(baseOffsets, cmp.Compare)

	for i := 0; i < len(baseOffsets); i++ {
		err = l.newSegment(baseOffsets[i])
		if err != nil {
			return err
		}
		// baseOffsets contains duplicate for index and store (both
		// share the same number in segment) so we skip the dup.
		i++
	}
	if l.segments == nil {
		err = l.newSegment(l.Config.Segment.InitialOffset)
		if err != nil {
			return err
		}
	}
	return err
}

// Append appends a record to the active segment of the log. A new segment
// is created if current segment is at its max size.
// This method is thread-safe with rwMutex synchronization.
//
// Returns the offset of the newly appended record in the Log.
func (l *Log) Append(record *protolog.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.curSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.curSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read returns the record stored at the offset (off). This method is
// thread-safe and allows concurrent read operations.
func (l *Log) Read(off uint64) (*protolog.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, seg := range l.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			s = seg
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, protolog.ErrOffsetOutOfRange{Offset: off}
	}
	// gets index entry from the segment's index, and read the data
	// from the store.
	return s.Read(off)
}

// newSegment creates a new Segment, appends it to the log's slice
// of segments, and makes the new Segment the active Segment.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Directory, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.curSegment = s
	return nil
}

// TruncatePrev removes all segments whose highest offset is lower than
// (lowest). TruncatePrev should be called on segments whose data has
// already been processed by then.
func (l *Log) TruncatePrev(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, seg := range l.segments {
		if seg.nextOffset <= lowest+1 {
			if err := seg.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, seg)
	}
	l.segments = segments
	return nil
}

/*
We'll need Reader() when we implement consensus and need to support
snapshots and restoring a log.
*/

// Reader returns an io.Reader to read the whole log.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, seg := range l.segments {
		// store is wrapped in originReader to (1) satisfy the io.Reader
		// interface and (2) to ensure we begin reading from the origin
		// of the store.
		reader := &originReader{
			offset: 0,
			store:  seg.store,
		}
		readers[i] = reader
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	offset int64
}

// Read reads len(p) bytes into (p) from originReader's current offset;
// increments offset by number of bytes read. Returns number of bytes
// read.
func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.offset)
	o.offset += int64(n)
	return n, err
}

// LowestOffset returns the 0th segment's baseOffset.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the last segment's nextOffset.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var (
		end = len(l.segments)
		off = l.segments[end-1].nextOffset
	)
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Close iterates over the segments and closes them.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Reset removes the existing log (wipes it from memory) and then
// creates a new log to replace it.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// Remove closes the log and then removes its data.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Directory)
}
