package gowal

import (
	"iter"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrExists             = errors.New("record with such index already exists")
	ErrNonSequentialIndex = errors.New("record index must be next in sequence")
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
// It is unsafe because it removes all the corrupted segment files (invalid frame trailer or checksum mismatch).
// It returns the list of segment files that were removed.
func UnsafeRecover(dir, segmentPrefix string) ([]string, error) {
	namer := segmentNamer{dir: dir, prefix: segmentPrefix}
	segmentsNumbers, err := findSegmentNumbers(dir, namer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	return removeCorruptedSegments(segmentsNumbers, namer)
}

// Get queries record at specific index in the log.
func (c *Wal) Get(index uint64) (Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	record, ok := c.segments.record(index)
	if ok {
		return record.clone(), nil
	}

	return Record{}, nil
}

// CurrentIndex returns current index of the log.
func (c *Wal) CurrentIndex() uint64 {
	return c.lastIndex.Load()
}

// Write writes record to the log.
func (c *Wal) Write(record Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	expectedIndex := c.lastIndex.Load() + 1
	if record.Index != expectedIndex {
		return errors.Wrapf(ErrNonSequentialIndex, "expected index %d, got %d", expectedIndex, record.Index)
	}

	record = NewRecord(record.Index, record.Key, record.Value)

	if err := c.checkIndexExists(record); err != nil {
		return err
	}

	if err := c.writeRecords([]Record{record}); err != nil {
		return err
	}

	c.lastIndex.Store(record.Index)
	return nil
}

// WriteBatch appends a batch of records to the WAL in a single operation.
func (c *Wal) WriteBatch(batch Batch) error {
	if batch.Len() == 0 {
		return nil
	}

	batch.sort()
	if err := batch.validate(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := batch.validateSequenceAfter(c.lastIndex.Load()); err != nil {
		return err
	}

	if err := c.checkIndexExists(batch.records...); err != nil {
		return err
	}

	recordsToWrite := make([]Record, 0, batch.Len())
	for _, r := range batch.records {
		recordsToWrite = append(recordsToWrite, NewRecord(r.Index, r.Key, r.Value))
	}

	if err := c.writeRecords(recordsToWrite); err != nil {
		return err
	}

	c.lastIndex.Add(uint64(batch.Len()))

	return nil
}

func (c *Wal) checkIndexExists(records ...Record) error {
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

	existingRecord, ok := c.segments.record(index)
	if !ok {
		return nil
	}

	tombstone := newTombstone(existingRecord)

	if err := c.writeRecords([]Record{tombstone}); err != nil {
		return err
	}

	c.segments.forgetHistorical(index)
	return nil
}

// writeRecords is an internal method that encodes and writes records to the log.
func (c *Wal) writeRecords(records []Record) error {
	if err := c.segments.append(records); err != nil {
		return err
	}

	if c.isInSyncDiskMode {
		if err := c.segments.syncActive(); err != nil {
			return errors.Wrap(err, "failed to sync log")
		}
	}

	return nil
}

// Iterator returns push-based iterator for the WAL records.
// Records are returned from the oldest to the newest.
//
// Should be used like this:
//
//	for record := range wal.Iterator() {
//		...
func (c *Wal) Iterator() iter.Seq[Record] {
	return func(yield func(Record) bool) {
		c.mu.RLock()

		records := c.segments.records()
		c.mu.RUnlock()

		slices.SortFunc(records, func(a, b Record) int {
			if a.Index < b.Index {
				return -1
			}
			if a.Index > b.Index {
				return 1
			}
			return 0
		})

		for _, record := range records {
			if !yield(record) {
				break
			}
		}
	}
}

// PullIterator returns pull-based iterator for the WAL records.
// Records are returned from the oldest to the newest.
//
// Should be used like this:
//
//	next, stop := wal.PullIterator()
//	defer stop()
//	...
func (c *Wal) PullIterator() (next func() (Record, bool), stop func()) {
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
