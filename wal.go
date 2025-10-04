package gowal

import (
	"bytes"
	"encoding/gob"
	"iter"
	"os"
	"path"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

var ErrExists = errors.New("msg with such index already exists")

// Wal is a write-ahead log that stores key-value pairs.
//
// Wal is append-only log, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
//
// Index stored in memory and loaded from disk on Wal init.
type Wal struct {
	// mutex for thread safety
	mu sync.RWMutex

	// append-only log with proposed messages that node consumed
	log *os.File

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

	lastIndex atomic.Uint64

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

// NewWAL creates a new WAL with the given configuration.
func NewWAL(config Config) (*Wal, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create log directory")
	}

	segmentsNumbers, err := findSegmentNumber(config.Dir, config.Prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	// load segments into mem
	fd, lastOffset, index, err := segmentInfoAndIndex(segmentsNumbers, path.Join(config.Dir, config.Prefix))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load log segments")
	}
	numberOfSegments := len(index) / config.SegmentThreshold
	if numberOfSegments == 0 {
		numberOfSegments = 1
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	w := &Wal{log: fd, index: index, tmpIndex: make(map[uint64]msg),
		buf: &buf, enc: enc, lastOffset: lastOffset, pathToLogsDir: config.Dir,
		segmentsNumber: numberOfSegments, prefix: config.Prefix, segmentsThreshold: config.SegmentThreshold,
		maxSegments: config.MaxSegments, isInSyncDiskMode: config.IsInSyncDiskMode}

	lastIndex := uint64(0)
	for v := range w.Iterator() {
		if v.Idx > lastIndex {
			lastIndex = v.Idx
		}
	}

	w.lastIndex.Store(lastIndex)

	return w, nil
}

// UnsafeRecover recovers the WAL from the given directory.
// It is unsafe because it removes all the segment and checksum files that are corrupted (checksums do not match).
// It returns the list of segment and checksum files that were removed.
func UnsafeRecover(dir, segmentPrefix string) ([]string, error) {
	segmentsNumbers, err := findSegmentNumber(dir, segmentPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	return removeCorruptedSegments(segmentsNumbers, path.Join(dir, segmentPrefix))
}

// Get queries value at specific index in the log.
func (c *Wal) Get(index uint64) (string, []byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// check main index first
	msg, ok := c.index[index]
	if ok {
		// verify checksum on read
		if err := msg.verifyChecksum(); err != nil {
			return "", nil, err
		}
		return msg.Key, msg.Value, nil
	}

	// check temporary index for current segment
	msg, ok = c.tmpIndex[index]
	if ok {
		// verify checksum on read
		if err := msg.verifyChecksum(); err != nil {
			return "", nil, err
		}
		return msg.Key, msg.Value, nil
	}

	return "", nil, nil
}

// CurrentIndex returns current index of the log.
func (c *Wal) CurrentIndex() uint64 {
	return c.lastIndex.Load()
}

// Write writes key-value pair to the log.
func (c *Wal) Write(index uint64, key string, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[index]; exists {
		return ErrExists
	}

	if err := c.rotateIfNeeded(); err != nil {
		return err
	}

	m := msg{Key: key, Value: value, Idx: index}
	m.Checksum = m.calculateChecksum()

	data, err := msgpack.Marshal(m)
	if err != nil {
		return errors.Wrap(err, "failed to encode msg")
	}

	if _, err := c.log.Write(data); err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}

	if c.isInSyncDiskMode {
		if err := c.log.Sync(); err != nil {
			return errors.Wrap(err, "failed to sync log")
		}
	}

	c.lastOffset += int64(len(data))
	c.lastIndex.Add(1)
	c.tmpIndex[index] = m

	return nil
}

// WriteTombstone writes a tombstone record for the given index.
// If no record exists for the index, returns nil (no-op).
// If a record exists, overwrites it with a tombstone.
func (c *Wal) WriteTombstone(index uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if record exists in main index or temp index
	var existingMsg msg
	var exists bool

	if msg, ok := c.index[index]; ok {
		existingMsg = msg
		exists = true
	} else if msg, ok := c.tmpIndex[index]; ok {
		existingMsg = msg
		exists = true
	}

	// if no record exists, return nil (no-op)
	if !exists {
		return nil
	}

	if err := c.rotateIfNeeded(); err != nil {
		return err
	}

	// create tombstone with existing key
	tombstone := msg{Key: existingMsg.Key, Value: []byte("tombstone"), Idx: index}
	tombstone.Checksum = tombstone.calculateChecksum()

	data, err := msgpack.Marshal(tombstone)
	if err != nil {
		return errors.Wrap(err, "failed to encode tombstone")
	}

	if _, err := c.log.Write(data); err != nil {
		return errors.Wrap(err, "failed to write tombstone to log")
	}

	if c.isInSyncDiskMode {
		if err := c.log.Sync(); err != nil {
			return errors.Wrap(err, "failed to sync log")
		}
	}

	c.lastOffset += int64(len(data))
	c.tmpIndex[index] = tombstone

	// remove from main index if it exists there to avoid confusion
	delete(c.index, index)

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
		c.mu.RLock()

		// collect indexes from both main index and tmpIndex
		allIndexes := make(map[uint64]msg, len(c.index)+len(c.tmpIndex))
		for k, v := range c.index {
			allIndexes[k] = v
		}
		for k, v := range c.tmpIndex {
			allIndexes[k] = v
		}

		msgIndexes := make([]uint64, 0, len(allIndexes))
		for k := range allIndexes {
			msgIndexes = append(msgIndexes, k)
		}

		// create a copy of messages to avoid holding lock during iteration
		msgs := make([]msg, len(msgIndexes))
		for i, idx := range msgIndexes {
			msgs[i] = allIndexes[idx]
		}
		c.mu.RUnlock()

		slices.SortFunc(msgs, func(a, b msg) int {
			if a.Idx < b.Idx {
				return -1
			}
			if a.Idx > b.Idx {
				return 1
			}
			return 0
		})

		for _, msg := range msgs {
			if !yield(msg) {
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

// Close closes log file.
func (c *Wal) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.log.Close(); err != nil {
		return errors.Wrap(err, "failed to close log file")
	}

	return nil
}
