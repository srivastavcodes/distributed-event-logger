package log

import (
	api "Proglog/api/v1"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu sync.RWMutex

	Directory     string
	Config        Config
	segments      []*segment
	activeSegment *segment
}

// NewLog creates a new Log instance with the specified directory and configuration.
// It sets default values for segment size limits if not provided, then initializes
// the log structure and sets up its initial segments for reading and writing records.
func NewLog(dir string, config Config) (*Log, error) {
	if config.Segment.MaxStoreBytes == 0 {
		config.Segment.MaxStoreBytes = 1024
	}
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = 1024
	}
	log := &Log{
		Config:    config,
		Directory: dir,
	}
	return log, log.setup()
}

// TODO -> write documentation for code below (all functions)
func (log *Log) setup() error {
	files, err := os.ReadDir(log.Directory)
	if err != nil {
		return err
	}
	var baseOffsets []uint64

	for _, f := range files {
		offStr := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	slices.Sort(baseOffsets)

	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains duplicate for index and store, so we skip one ahead
		i++
	}
	if log.segments == nil {
		if err = log.newSegment(log.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) newSegment(off uint64) error {
	seg, err := newSegment(log.Directory, off, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, seg)
	log.activeSegment = seg
	return nil
}

func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if log.activeSegment.IsMaxed() {
		err = log.newSegment(off + 1)
	}
	return off, err
}

func (log *Log) Read(off uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()

	var seg *segment
	for _, sg := range log.segments {
		if sg.baseOffset <= off && off < sg.nextOffset {
			seg = sg
			break
		}
	}
	if seg == nil || seg.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return seg.Read(off)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()

	for _, seg := range log.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Directory)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	off := log.segments[len(log.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	var segments []*segment
	for _, seg := range log.segments {
		if seg.nextOffset <= lowest+1 {
			if err := seg.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, seg)
	}
	log.segments = segments
	return nil
}

func (log *Log) Reader() io.Reader {
	log.mu.Lock()
	defer log.mu.Unlock()

	readers := make([]io.Reader, len(log.segments))
	for i, seg := range log.segments {
		readers[i] = &originReader{
			store: seg.store, off: 0,
		}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (or *originReader) Read(data []byte) (int, error) {
	n, err := or.ReadAt(data, or.off)
	or.off += int64(n)
	return n, err
}
