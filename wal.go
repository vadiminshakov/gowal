package gowal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"iter"
	"os"
	"path"
	"sort"
)

var ErrExists = errors.New("msg with such index already exists")

// Wal is a write-ahead log that stores key-value pairs.
//
// Wal is append-only log, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
//
// Index stored in memory and loaded from disk on Wal init.
type Wal struct {
	// append-only log with proposed messages that node consumed
	log *os.File

	// file with checksum for current segment
	checksum *os.File

	// index that matches height of msg record with offset in file
	index    map[uint64]msg
	tmpIndex map[uint64]msg

	// gob encoder for proposed messages
	enc *gob.Encoder

	// buffer for proposed messages
	buf *bytes.Buffer

	// path to directory with logs
	pathToLogsDir string

	// name of the old segment
	oldestSegName string

	// offset of last record in file
	lastOffset int64

	// number of segments for log
	segmentsNumber int

	// prefix for segment files
	prefix string

	segmentsThreshold int

	maxSegments int

	isInSyncDiskMode bool
}

// Config represents the configuration for the WAL (Write-Ahead Log).
type Config struct {
	// Dir is the directory where the log files will be stored.
	Dir string

	// Prefix is the prefix for the segment files.
	Prefix string

	// SegmentThreshold is the number of records after which a new segment is created.
	SegmentThreshold int

	// MaxSegments is the maximum number of segments allowed before the oldest segment is deleted.
	MaxSegments int

	// IsInSyncDiskMode indicates whether the log should be synced to disk after each write.
	IsInSyncDiskMode bool
}

func NewWAL(config Config) (*Wal, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create log directory")
	}

	segmentsNumbers, err := findSegmentNumber(config.Dir, config.Prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	// load segments into mem
	fd, chk, stat, index, err := segmentInfoAndIndex(segmentsNumbers, path.Join(config.Dir, config.Prefix))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load log segments")
	}
	numberOfSegments := len(index) / config.SegmentThreshold
	if numberOfSegments == 0 {
		numberOfSegments = 1
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	return &Wal{log: fd, index: index, checksum: chk, tmpIndex: make(map[uint64]msg),
		buf: &buf, enc: enc, lastOffset: stat.Size(), pathToLogsDir: config.Dir,
		segmentsNumber: numberOfSegments, prefix: config.Prefix, segmentsThreshold: config.SegmentThreshold,
		maxSegments: config.MaxSegments, isInSyncDiskMode: config.IsInSyncDiskMode}, nil
}

// Write writes key-value pair to the log.
func (c *Wal) Write(index uint64, key string, value []byte) error {
	if _, exists := c.index[index]; exists {
		return ErrExists // Предотвращаем дублирование индексов
	}

	if err := c.rotateIfNeeded(index, key, value); err != nil {
		return err
	}

	if err := c.enc.Encode(msg{Key: key, Value: value, Idx: index}); err != nil {
		return errors.Wrap(err, "failed to encode msg")
	}

	if _, err := c.log.WriteAt(c.buf.Bytes(), c.lastOffset); err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}

	if err := writeChecksum(c.log, c.checksum); err != nil {
		return errors.Wrap(err, "failed to write checksum")
	}

	fmt.Printf("Wrote message at offset %d: index=%d, key=%s to segment %s\n", c.lastOffset, index, key, c.log.Name())

	if c.isInSyncDiskMode {
		if err := c.log.Sync(); err != nil {
			return errors.Wrap(err, "failed to sync log")
		}
		if err := c.checksum.Sync(); err != nil {
			return errors.Wrap(err, "failed to checksum")
		}
	}

	c.lastOffset += int64(c.buf.Len())
	c.buf.Reset()
	c.index[index] = msg{Key: key, Value: value, Idx: index}

	return nil
}

// Iterator returns push-based iterator for the WAL messages.
// Messages are returned from the oldest to the newest.
//
// Should be used like this:
//
//	for msg := range wal.Iterator() {
//		...
func (c *Wal) Iterator() iter.Seq[msg] {
	return func(yield func(msg) bool) {
		msgIndexes := make([]uint64, 0, len(c.index))

		for k := range c.index {
			msgIndexes = append(msgIndexes, k)
		}

		sort.Slice(msgIndexes, func(i, j int) bool {
			return msgIndexes[i] < msgIndexes[j]
		})

		for i := 0; i < len(msgIndexes); i++ {
			if !yield(c.index[msgIndexes[i]]) {
				break
			}
		}
	}
}

// PullIterator returns pull-based iterator for the WAL messages.
// Messages are returned from the oldest to the newest.
//
// Should be used like this:
//
//	next, stop := wal.PullIterator()
//	defer stop()
//	...
func (c *Wal) PullIterator() (next func() (msg, bool), stop func()) {
	return iter.Pull(c.Iterator())
}

// CurrentIndex returns current index of the log.
func (c *Wal) CurrentIndex() uint64 {
	return uint64(len(c.index))
}

// Close closes log and checksum files.
func (c *Wal) Close() error {
	if err := c.log.Close(); err != nil {
		return errors.Wrap(err, "failed to close log log file")
	}

	if err := c.checksum.Close(); err != nil {
		return errors.Wrap(err, "failed to close checksum file")
	}

	return nil
}
