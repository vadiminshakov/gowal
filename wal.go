package gowal

import (
	stderrors "errors"
	"fmt"
	"iter"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

const logDir = "log"

var (
	ErrExists             = errors.New("record with such index already exists")
	ErrNonSequentialIndex = errors.New("record index must be next in sequence")

	// ErrFailed is returned by all writes after writing, syncing or removing a segment has failed.
	// The WAL must be closed and opened again.
	ErrFailed = errors.New("wal is failed after an I/O error, reopen it")
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

	// failed is the error that stopped the log, or nil
	failed error
}

// Config represents the configuration for the WAL (Write-Ahead Log).
type Config struct {
	// Prefix is the prefix for the segment files.
	Prefix string

	// SegmentThreshold is the number of records after which a new segment is created.
	SegmentThreshold int

	// MaxSegments is the maximum number of segments allowed before the oldest segment is deleted.
	MaxSegments int

	// IsInSyncDiskMode indicates whether the log should be synced to disk after each write.
	IsInSyncDiskMode bool
}

// NewWAL creates a WAL in ./log with the given configuration.
//
// A torn last frame in the last segment (left by a crash in the middle of a write) is truncated.
// Corruption anywhere else returns an error, and so does a gap in indexes (ErrIndexGap).
// Such a log can be cut to its valid prefix with UnsafeRecover.
func NewWAL(config Config) (*Wal, error) {
	if err := os.Mkdir(logDir, 0755); err != nil && !os.IsExist(err) {
		return nil, errors.Wrap(err, "failed to create log directory")
	}
	if err := syncDir("."); err != nil {
		return nil, err
	}

	segments, lastIndex, err := openSegmentSet(config)
	if err != nil {
		return nil, err
	}
	if err := syncDir(logDir); err != nil {
		return nil, stderrors.Join(err, segments.close())
	}

	w := &Wal{
		segments:         segments,
		isInSyncDiskMode: config.IsInSyncDiskMode,
	}

	w.lastIndex.Store(lastIndex)

	return w, nil
}

// UnsafeRecover recovers the WAL in ./log, so that NewWAL can open it.
//
// It cuts the log at the first broken frame or index gap and removes everything after it,
// including all later segments. The remaining log is a contiguous prefix without holes.
// If a frame contains a batch, the entire batch is dropped.
// It is unsafe because the dropped records are lost, even if they are valid.
//
// A broken frame or gap is not an error: it is reported in RecoverResult.
// An error is returned if the log files can't be read, truncated, removed or synced,
// or if a directory change can't be synced.
func UnsafeRecover(segmentPrefix string) (RecoverResult, error) {
	namer := segmentNamer{dir: logDir, prefix: segmentPrefix}
	segmentsNumbers, err := findSegmentNumbers(logDir, namer)
	if err != nil {
		return RecoverResult{}, errors.Wrap(err, "failed to find segment numbers")
	}

	return cutLog(segmentsNumbers, namer)
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

	if c.failed != nil {
		return c.failed
	}

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

// WriteBatch appends a batch in one frame, so recovery keeps or drops it as a whole.
// The encoded batch must not exceed 16 MiB; otherwise it returns ErrRecordTooLarge
// without changing the log. An empty batch is a no-op.
func (c *Wal) WriteBatch(batch Batch) error {
	if batch.Len() == 0 {
		return nil
	}

	workingBatch := Batch{records: batch.Records()}
	workingBatch.sort()
	if err := workingBatch.validate(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failed != nil {
		return c.failed
	}

	if err := workingBatch.validateSequenceAfter(c.lastIndex.Load()); err != nil {
		return err
	}

	if err := c.checkIndexExists(workingBatch.records...); err != nil {
		return err
	}

	recordsToWrite := make([]Record, 0, workingBatch.Len())
	for _, r := range workingBatch.records {
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

	if c.failed != nil {
		return c.failed
	}

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
// Any error except a too large record or batch or a failed creation of a new segment stops the log:
// the file state after a failed write or sync is unknown, and retrying fsync is unsafe
// because the kernel may have already dropped the dirty pages.
func (c *Wal) writeRecords(records []Record) error {
	err := c.segments.append(records, c.isInSyncDiskMode)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrRecordTooLarge) || errors.Is(err, errRotation) {
		return err
	}

	c.failed = fmt.Errorf("%w: %w", ErrFailed, err)
	return c.failed
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

// Close syncs pending records and closes the log.
func (c *Wal) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var syncErr error
	if c.failed == nil {
		if err := c.segments.active.Sync(); err != nil {
			syncErr = fmt.Errorf("%w: failed to sync on close: %w", ErrFailed, err)
			c.failed = syncErr
		}
	}
	return stderrors.Join(syncErr, c.segments.close())
}
