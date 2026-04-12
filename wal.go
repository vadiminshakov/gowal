package gowal

import (
	"bytes"
	"iter"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrExists             = errors.New("msg with such index already exists")
	ErrNonSequentialIndex = errors.New("msg index must be next in sequence")
)

// Wal is a write-ahead log that stores key-value pairs.
//
// Wal is append-only log, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
//
// Index stored in memory and loaded from disk on Wal init.
type Wal struct {
	// mutex for thread safety
	mu sync.RWMutex

	segments *segmentSet

	lastIndex atomic.Uint64

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

// Record is a struct that represents a record in the log.
type Record struct {
	// Index is the unique index of the record for fast search.
	Index uint64
	// Key is the unique key of the record.
	Key string
	// Value is the value of the record.
	Value []byte
}

// NewWAL creates a new WAL with the given configuration.
func NewWAL(config Config) (*Wal, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create log directory")
	}

	segments, lastIndex, err := openSegmentSet(config)
	if err != nil {
		return nil, err
	}

	w := &Wal{
		segments:         segments,
		isInSyncDiskMode: config.IsInSyncDiskMode,
	}

	w.lastIndex.Store(lastIndex)

	return w, nil
}

// UnsafeRecover recovers the WAL from the given directory.
// It is unsafe because it removes all the segment and checksum files that are corrupted (checksums do not match).
// It returns the list of segment and checksum files that were removed.
func UnsafeRecover(dir, segmentPrefix string) ([]string, error) {
	namer := segmentNamer{dir: dir, prefix: segmentPrefix}
	segmentsNumbers, err := findSegmentNumbers(dir, namer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	return removeCorruptedSegments(segmentsNumbers, namer)
}

// Get queries value at specific index in the log.
func (c *Wal) Get(index uint64) (string, []byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	msg, ok := c.segments.record(index)
	if ok {
		// verify checksum on read
		if err := msg.verifyChecksum(); err != nil {
			return "", nil, err
		}
		return msg.Key, bytes.Clone(msg.Value), nil
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

	expectedIndex := c.lastIndex.Load() + 1
	if index != expectedIndex {
		return errors.Wrapf(ErrNonSequentialIndex, "expected index %d, got %d", expectedIndex, index)
	}

	if _, exists := c.segments.record(index); exists {
		return ErrExists
	}

	m := newMsg(index, key, value)

	if err := c.writeMessages([]msg{m}); err != nil {
		return err
	}

	c.lastIndex.Store(index)
	return nil
}

// WriteBatch appends a batch of records to the WAL in a single operation.
func (c *Wal) WriteBatch(batch Batch) error {
	if batch.Len() == 0 {
		return nil
	}

	if !batch.createdWithConstructor {
		return errors.New("batch must be created with NewBatch constructor")
	}

	records := batch.Records()

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := batch.validateSequenceAfter(c.lastIndex.Load()); err != nil {
		return err
	}

	if err := c.checkExternalCollisions(records); err != nil {
		return err
	}

	messages := make([]msg, 0, batch.Len())
	for _, r := range records {
		messages = append(messages, newMsg(r.Index, r.Key, r.Value))
	}

	if err := c.writeMessages(messages); err != nil {
		return err
	}

	c.lastIndex.Add(uint64(batch.Len()))

	return nil
}

func (c *Wal) checkExternalCollisions(records []Record) error {
	for _, record := range records {
		if _, exists := c.segments.record(record.Index); exists {
			return ErrExists
		}
	}
	return nil
}

// WriteTombstone writes a tombstone record for the given index.
// If no record exists for the index, returns nil (no-op).
// If a record exists, overwrites it with a tombstone.
func (c *Wal) WriteTombstone(index uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	existingMsg, ok := c.segments.record(index)
	if !ok {
		return nil
	}

	tombstone := newTombstone(existingMsg)

	if err := c.writeMessages([]msg{tombstone}); err != nil {
		return err
	}

	c.segments.forgetHistorical(index)
	return nil
}

// writeMessages is an internal method that encodes and writes messages to the log.
func (c *Wal) writeMessages(messages []msg) error {
	if err := c.segments.append(messages); err != nil {
		return err
	}

	if c.isInSyncDiskMode {
		if err := c.segments.syncActive(); err != nil {
			return errors.Wrap(err, "failed to sync log")
		}
	}

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

		msgs := c.segments.records()
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

	if err := c.segments.close(); err != nil {
		return errors.Wrap(err, "failed to close log file")
	}

	return nil
}
