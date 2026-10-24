package gowal

import (
	"cmp"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteAndGet(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	for i := 1; i <= 10; i++ {
		record, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), record.Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(record.Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestWriteCopiesValue(t *testing.T) {
	dir := t.TempDir()
	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	value := []byte("value")
	require.NoError(t, log.Write(NewRecord(1, "key", value)))
	value[0] = 'X'

	got, err := log.Get(1)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), got.Value)

	got.Value[0] = 'Y'
	gotAgain, err := log.Get(1)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), gotAgain.Value)
}

func TestIteratorCopiesValue(t *testing.T) {
	dir := t.TempDir()
	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	require.NoError(t, log.Write(NewRecord(1, "key", []byte("value"))))

	for record := range log.Iterator() {
		record.Value[0] = 'X'
	}

	got, err := log.Get(1)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), got.Value)
}

func TestWriteRequiresSequentialIndexStartingAtOne(t *testing.T) {
	dir := t.TempDir()
	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()

	require.ErrorIs(t, log.Write(NewRecord(0, "key0", []byte("value0"))), ErrNonSequentialIndex)
	require.Equal(t, uint64(0), log.CurrentIndex())

	require.NoError(t, log.Write(NewRecord(1, "key1", []byte("value1"))))
	require.Equal(t, uint64(1), log.CurrentIndex())

	require.ErrorIs(t, log.Write(NewRecord(3, "key3", []byte("value3"))), ErrNonSequentialIndex)
	require.Equal(t, uint64(1), log.CurrentIndex())

	require.NoError(t, log.Write(NewRecord(2, "key2", []byte("value2"))))
	require.Equal(t, uint64(2), log.CurrentIndex())
}

func TestCurrentIndexTracksSequentialBatch(t *testing.T) {
	dir := t.TempDir()
	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	batch := NewBatch(
		NewRecord(1, "key1", []byte("value1")),
		NewRecord(2, "key2", []byte("value2")),
		NewRecord(3, "key3", []byte("value3")),
	)
	require.NoError(t, log.WriteBatch(batch))
	require.Equal(t, uint64(3), log.CurrentIndex())
	require.NoError(t, log.Close())

	log, err = NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(3), log.CurrentIndex())
}

func TestIterator(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	t.Run("Iterator", func(t *testing.T) {
		i := 1
		for m := range log.Iterator() {
			require.Equal(t, "key"+strconv.Itoa(i), m.Key)
			require.Equal(t, "value"+strconv.Itoa(i), string(m.Value))
			i++
		}
	})

	t.Run("PullIterator", func(t *testing.T) {
		iter, stop := log.PullIterator()
		defer stop()

		i := 1
		for {
			record, ok := iter()
			if !ok {
				break
			}

			require.Equal(t, "key"+strconv.Itoa(i), record.Key)
			require.Equal(t, "value"+strconv.Itoa(i), string(record.Value))
			i++
		}
	})

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestLoadIndex(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	index, _, _, err := log.segments.active.loadIndex(false, &indexSequence{})
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		require.Equal(t, "key"+strconv.Itoa(i), index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestSegmentRotation(t *testing.T) {
	segmentThreshold := 10
	segmentsNumber := 2

	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: segmentThreshold,
		MaxSegments:      segmentsNumber,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	// here we exceed the segment size threshold (segmentThreshold) and write 10 more log entries to the new segment
	for i := 1; i <= segmentThreshold*segmentsNumber+10; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// load index of last segment
	index, _, _, err := log.segments.active.loadIndex(false, &indexSequence{})
	require.NoError(t, err)

	// check
	indexValues := slices.Collect(maps.Values(index))

	slices.SortFunc(indexValues, func(a, b Record) int {
		return cmp.Compare(a.Index, b.Index)
	})

	for i, v := range indexValues {
		keyWithOffset := i + segmentThreshold*segmentsNumber + 1
		require.Equal(t, "value"+strconv.Itoa(keyWithOffset), string(v.Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

// create two segments, app down, up and repair index.
func TestServiceDownUpAndRepairIndex(t *testing.T) {
	segmentThreshold := 10

	initWal := func() (*Wal, error) {
		t.Helper()

		return NewWAL(Config{
			Dir:              "./testlogdata",
			Prefix:           "log_",
			SegmentThreshold: segmentThreshold,
			MaxSegments:      5,
			IsInSyncDiskMode: false,
		})
	}

	// init
	log, err := initWal()
	require.NoError(t, err)

	// create first segment
	for i := 1; i <= segmentThreshold; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// create second segment
	for i := segmentThreshold + 1; i <= segmentThreshold*2; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// close the wal
	require.NoError(t, log.Close())

	// open the wal again
	log, err = initWal()
	require.NoError(t, err)

	// check
	for i := 1; i <= segmentThreshold*2; i++ {
		record, ok := log.segments.record(uint64(i))
		require.True(t, ok)
		require.Equal(t, "key"+strconv.Itoa(i), record.Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(record.Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestRotationAfterRestartWithMissingOldSegment(t *testing.T) {
	dir := t.TempDir()
	const segmentThreshold = 2

	open := func() (*Wal, error) {
		t.Helper()
		return NewWAL(Config{
			Dir:              dir,
			Prefix:           "log_",
			SegmentThreshold: segmentThreshold,
			MaxSegments:      5,
			IsInSyncDiskMode: false,
		})
	}

	log, err := open()
	require.NoError(t, err)

	for i := 1; i <= 8; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}
	require.NoError(t, log.Close())
	require.NoError(t, os.Remove(segmentPath(dir, "log_", 0)))

	log, err = open()
	require.NoError(t, err)
	defer log.Close()

	for i := 9; i <= 10; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	require.NoFileExists(t, segmentPath(dir, "log_", 0))
	require.FileExists(t, segmentPath(dir, "log_", 1))
	require.FileExists(t, segmentPath(dir, "log_", 4))
}

func TestCurrentIndexDoesNotRegressWhenActiveSegmentHasOnlyOldTombstone(t *testing.T) {
	dir := t.TempDir()

	open := func() (*Wal, error) {
		t.Helper()
		return NewWAL(Config{
			Dir:              dir,
			Prefix:           "log_",
			SegmentThreshold: 2,
			MaxSegments:      10,
			IsInSyncDiskMode: false,
		})
	}

	log, err := open()
	require.NoError(t, err)

	require.NoError(t, log.Write(NewRecord(1, "key1", []byte("value1"))))
	require.NoError(t, log.Write(NewRecord(2, "key2", []byte("value2"))))
	require.NoError(t, log.WriteTombstone(1))
	require.Equal(t, uint64(2), log.CurrentIndex())
	require.NoError(t, log.Close())

	log, err = open()
	require.NoError(t, err)
	defer log.Close()

	require.Equal(t, uint64(2), log.CurrentIndex())
	require.NoError(t, log.Write(NewRecord(3, "key3", []byte("value3"))))
}

func TestChecksum(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// verify data can be read correctly (checksums verified on read)
	for i := 1; i <= 2; i++ {
		record, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), record.Key)
		require.Equal(t, []byte("value"+strconv.Itoa(i)), record.Value)
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestChecksum_Corrupted(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// close and re-open to test corruption detection
	log.Close()

	// corrupt the data by writing some garbage to the segment file
	f, err := os.OpenFile("./testlogdata/log_0", os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	f.Write([]byte("corrupted data"))
	f.Close()

	// try to reload - should fail due to checksum mismatch
	_, err = NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.Error(t, err)

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestChecksum_Check_Segment_Files(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 1,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// verify all data can be read correctly (checksums verified on read)
	for i := 1; i <= 5; i++ {
		record, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), record.Key)
		require.Equal(t, []byte("value"+strconv.Itoa(i)), record.Value)
	}

	// find all segment files (no more separate checksum files)
	files, err := os.ReadDir("./testlogdata")
	require.NoError(t, err)

	countOfSegments := 0
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		countOfSegments++
	}

	require.Equal(t, 5, countOfSegments)

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestChecksum_UnsafeRecover(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 2,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		require.NoError(t, log.Write(NewRecord(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))))
	}

	// close and corrupt the data by writing some garbage to the segment file
	log.Close()
	f, err := os.OpenFile("./testlogdata/log_4", os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	f.Write([]byte("corrupted data"))
	f.Close()

	// recover - should cut the garbage off the last segment
	result, err := UnsafeRecover("./testlogdata", "log_")
	require.NoError(t, err)
	require.Equal(t, RecoverResult{Truncated: "testlogdata/log_4", LastIndex: 10}, result)

	log, err = NewWAL(Config{Dir: "./testlogdata", Prefix: "log_", SegmentThreshold: 2, MaxSegments: 5})
	require.NoError(t, err)
	require.Equal(t, uint64(10), log.CurrentIndex())
	require.NoError(t, log.Close())

	require.NoError(t, os.RemoveAll("./testlogdata"))
}
func TestWriteTombstone(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer os.RemoveAll("./testlogdata")

	t.Run("WriteTombstone for non-existent record returns nil", func(t *testing.T) {
		// try to write tombstone for non-existent record
		err := log.WriteTombstone(999)
		require.NoError(t, err)

		// verify record doesn't exist
		record, err := log.Get(999)
		require.NoError(t, err)
		require.Equal(t, "", record.Key)
		require.Nil(t, record.Value)
	})

	t.Run("WriteTombstone for existing record", func(t *testing.T) {
		// write a record
		err := log.Write(NewRecord(1, "key1", []byte("value1")))
		require.NoError(t, err)

		// verify record exists
		record, err := log.Get(1)
		require.NoError(t, err)
		require.Equal(t, "key1", record.Key)
		require.Equal(t, "value1", string(record.Value))

		// write tombstone
		err = log.WriteTombstone(1)
		require.NoError(t, err)

		// verify tombstone exists with same key
		record, err = log.Get(1)
		require.NoError(t, err)
		require.Equal(t, "key1", record.Key)
		require.Equal(t, "tombstone", string(record.Value))
	})

	t.Run("WriteTombstone appears in iterator", func(t *testing.T) {
		// write some records
		err := log.Write(NewRecord(2, "key2", []byte("value2")))
		require.NoError(t, err)
		err = log.Write(NewRecord(3, "key3", []byte("value3")))
		require.NoError(t, err)
		err = log.Write(NewRecord(4, "key4", []byte("value4")))
		require.NoError(t, err)

		// write tombstone for one of them
		err = log.WriteTombstone(3)
		require.NoError(t, err)

		// check iterator contains tombstone
		found := false
		for record := range log.Iterator() {
			if record.Index == 3 {
				require.Equal(t, "key3", record.Key)
				require.Equal(t, "tombstone", string(record.Value))
				found = true
			}
		}
		require.True(t, found, "Tombstone should appear in iterator")
	})

	t.Run("WriteTombstone overwrites record from main index", func(t *testing.T) {
		// simulate having a record in main index by manually adding it
		// (this simulates the case where record was persisted to main index)
		record := NewRecord(5, "key5", []byte("value5"))
		log.mu.Lock()
		log.segments.historicalIndex[5] = record
		log.segments.historicalIndexBySegment[0] = map[uint64]Record{5: record}
		log.mu.Unlock()

		// verify record exists in main index
		record, err := log.Get(5)
		require.NoError(t, err)
		require.Equal(t, "key5", record.Key)
		require.Equal(t, "value5", string(record.Value))

		// write tombstone
		err = log.WriteTombstone(5)
		require.NoError(t, err)

		// verify tombstone overwrites the record
		record, err = log.Get(5)
		require.NoError(t, err)
		require.Equal(t, "key5", record.Key)
		require.Equal(t, "tombstone", string(record.Value))

		// verify record is removed from main index
		log.mu.RLock()
		_, existsInMainIndex := log.segments.historicalIndex[5]
		_, existsInTmpIndex := log.segments.active.index[5]
		log.mu.RUnlock()

		require.False(t, existsInMainIndex, "Record should be removed from main index")
		require.True(t, existsInTmpIndex, "Tombstone should exist in temp index")
	})
}

func TestWriteBatchAcceptsUnsortedSequentialRecords(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer wal.Close()

	batch := NewBatch(
		NewRecord(3, "key3", []byte("value3")),
		NewRecord(1, "key1", []byte("value1")),
		NewRecord(2, "key2", []byte("value2")),
	)
	require.NoError(t, wal.WriteBatch(batch))
	require.Equal(t, uint64(3), wal.CurrentIndex())
}

func TestWriteBatch_NonSequentialWithCurrentIndex(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	// Write a single entry first
	require.NoError(t, wal.Write(NewRecord(1, "initial", []byte("value"))))

	batch := NewBatch(
		NewRecord(3, "key3", []byte("value3")),
		NewRecord(4, "key4", []byte("value4")),
	)

	err = wal.WriteBatch(batch)
	require.ErrorIs(t, err, ErrNonSequentialIndex, "expected ErrNonSequentialIndex for a skipped index")
}

func TestWriteBatch_RotateWhenTmpIndexAlreadyFull(t *testing.T) {
	dir := t.TempDir()
	segmentThreshold := 3

	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: segmentThreshold,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer wal.Close()

	for i := 1; i <= segmentThreshold; i++ {
		require.NoError(t, wal.Write(NewRecord(uint64(i), fmt.Sprintf("k%d", i), []byte("v"))))
	}
	require.Equal(t, segmentThreshold, wal.segments.active.Len())

	batch := NewBatch(
		NewRecord(4, "k4", []byte("v4")),
		NewRecord(5, "k5", []byte("v5")),
	)
	require.NoError(t, wal.WriteBatch(batch))

	for i := 1; i <= segmentThreshold; i++ {
		_, ok := wal.segments.historicalIndex[uint64(i)]
		require.True(t, ok)
	}

	for _, r := range batch.Records() {
		_, ok := wal.segments.active.Record(r.Index)
		require.True(t, ok)
	}
}

func TestWriteBatchDoesNotMutateInputBatch(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer wal.Close()

	batch := NewBatch(
		NewRecord(3, "key3", []byte("value3")),
		NewRecord(1, "key1", []byte("value1")),
		NewRecord(2, "key2", []byte("value2")),
	)

	require.NoError(t, wal.WriteBatch(batch))

	records := batch.Records()
	require.Equal(t, []uint64{3, 1, 2}, []uint64{records[0].Index, records[1].Index, records[2].Index})
}

func TestRotationThresholdCountsAppendedRecords(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 2,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer wal.Close()

	require.NoError(t, wal.Write(NewRecord(1, "key1", []byte("value1"))))
	require.NoError(t, wal.Write(NewRecord(2, "key2", []byte("value2"))))

	require.NoError(t, wal.WriteTombstone(1))
	require.NoError(t, wal.WriteTombstone(1))
	require.NoError(t, wal.WriteTombstone(1))

	require.FileExists(t, segmentPath(dir, "log_", 0))
	require.FileExists(t, segmentPath(dir, "log_", 1))
	require.FileExists(t, segmentPath(dir, "log_", 2))
}

func TestOpenTruncatesPartialLastFrame(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 100, 3)
	validSize := fileSize(t, path)

	frame, err := codec{}.marshal(NewRecord(4, "key", []byte("value")))
	require.NoError(t, err)
	appendToSegment(t, path, frame[:len(frame)/2])

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.NoError(t, err)
	require.Equal(t, uint64(3), log.CurrentIndex())
	require.Equal(t, validSize, fileSize(t, path))

	require.NoError(t, log.Write(NewRecord(4, "key", []byte("value"))))
	require.NoError(t, log.Close())

	log, err = NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(4), log.CurrentIndex())
	record, err := log.Get(4)
	require.NoError(t, err)
	require.Equal(t, NewRecord(4, "key", []byte("value")), record)
}

func TestOpenTruncatesZeroFilledTail(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 100, 3)
	validSize := fileSize(t, path)

	appendToSegment(t, path, make([]byte, 64))

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(3), log.CurrentIndex())
	require.Equal(t, validSize, fileSize(t, path))
}

func TestOpenTruncatesBrokenFrameAtEOF(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 100, 2)
	validSize := fileSize(t, path)

	frame, err := codec{}.marshal(NewRecord(3, "key", []byte("value")))
	require.NoError(t, err)
	frame[frameHeaderSize] ^= 0xFF
	appendToSegment(t, path, frame)

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(2), log.CurrentIndex())
	require.Equal(t, validSize, fileSize(t, path))
}

func TestOpenFailsOnCorruptionInTheMiddle(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 100, 3)

	frame, err := codec{}.marshal(NewRecord(1, "key", []byte("value")))
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(frame)+frameHeaderSize] ^= 0xFF // payload of the second frame
	require.NoError(t, os.WriteFile(path, data, 0644))

	_, err = NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
	require.Equal(t, int64(len(data)), fileSize(t, path))
}

func TestOpenFailsOnTornTailInNotLastSegment(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 2, 3)

	require.NoError(t, os.Truncate(path, fileSize(t, path)-1))

	_, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 2, IsInSyncDiskMode: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "partial frame")
}

func TestOpenTruncatesHeaderOnlyTail(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 100, 3)
	validSize := fileSize(t, path)

	frame, err := codec{}.marshal(NewRecord(4, "key", []byte("value")))
	require.NoError(t, err)
	appendToSegment(t, path, frame[:frameHeaderSize])

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(3), log.CurrentIndex())
	require.Equal(t, validSize, fileSize(t, path))
}

func TestOpenFailsOnGapBetweenSegments(t *testing.T) {
	dir := t.TempDir()
	writeRecords(t, dir, 2, 6)
	require.NoError(t, os.Remove(segmentPath(dir, "log_", 1)))

	_, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 2, IsInSyncDiskMode: true})
	require.ErrorIs(t, err, ErrIndexGap)
}

func TestOpenFailsOnGapInsideSegment(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, "log_", 0)
	writeRecords(t, dir, 100, 2)

	frame, err := codec{}.marshal(NewRecord(4, "key", []byte("value")))
	require.NoError(t, err)
	appendToSegment(t, path, frame)
	size := fileSize(t, path)

	_, err = NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.ErrorIs(t, err, ErrIndexGap)
	require.Equal(t, size, fileSize(t, path))
}

func TestUnsafeRecoverDropsEverythingAfterBrokenFirstFrame(t *testing.T) {
	dir := t.TempDir()
	writeRecords(t, dir, 2, 6)

	path := segmentPath(dir, "log_", 1)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[frameHeaderSize] ^= 0xFF // payload of the first frame
	require.NoError(t, os.WriteFile(path, data, 0644))

	result, err := UnsafeRecover(dir, "log_")
	require.NoError(t, err)
	require.Equal(t, RecoverResult{
		Removed:   []string{segmentPath(dir, "log_", 1), segmentPath(dir, "log_", 2)},
		LastIndex: 2,
	}, result)

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 2, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(2), log.CurrentIndex())
	require.NoError(t, log.Write(NewRecord(3, "key", []byte("value"))))
}

func TestUnsafeRecoverTruncatesAtBrokenFrameInTheMiddle(t *testing.T) {
	dir := t.TempDir()
	writeRecords(t, dir, 2, 6)

	path := segmentPath(dir, "log_", 1)
	frame, err := codec{}.marshal(NewRecord(3, "key", []byte("value")))
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(frame)+frameHeaderSize] ^= 0xFF // payload of the second frame
	require.NoError(t, os.WriteFile(path, data, 0644))

	result, err := UnsafeRecover(dir, "log_")
	require.NoError(t, err)
	require.Equal(t, RecoverResult{
		Truncated: path,
		Removed:   []string{segmentPath(dir, "log_", 2)},
		LastIndex: 3,
	}, result)
	require.Equal(t, int64(len(frame)), fileSize(t, path))

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 2, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(3), log.CurrentIndex())
}

func TestUnsafeRecoverCutsLogAtGapBetweenSegments(t *testing.T) {
	dir := t.TempDir()
	writeRecords(t, dir, 2, 6)
	require.NoError(t, os.Remove(segmentPath(dir, "log_", 1)))

	result, err := UnsafeRecover(dir, "log_")
	require.NoError(t, err)
	require.Equal(t, RecoverResult{
		Removed:   []string{segmentPath(dir, "log_", 2)},
		LastIndex: 2,
	}, result)

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 2, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()
	require.Equal(t, uint64(2), log.CurrentIndex())
}

func TestUnsafeRecoverKeepsValidLogWithTombstones(t *testing.T) {
	dir := t.TempDir()
	writeRecords(t, dir, 2, 4)

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: 2, IsInSyncDiskMode: true})
	require.NoError(t, err)
	require.NoError(t, log.WriteTombstone(1))
	require.NoError(t, log.WriteTombstone(4))
	require.NoError(t, log.Close())

	result, err := UnsafeRecover(dir, "log_")
	require.NoError(t, err)
	require.Equal(t, RecoverResult{LastIndex: 4}, result)
}

func TestTooLargeRecordDoesNotStopLog(t *testing.T) {
	log, err := NewWAL(Config{Dir: t.TempDir(), Prefix: "log_", SegmentThreshold: 100, IsInSyncDiskMode: true})
	require.NoError(t, err)
	defer log.Close()

	err = log.Write(NewRecord(1, "key", make([]byte, maxFrameDataSize)))
	require.ErrorIs(t, err, ErrRecordTooLarge)
	require.NotErrorIs(t, err, ErrFailed)

	require.NoError(t, log.Write(NewRecord(1, "key", []byte("value"))))
	require.Equal(t, uint64(1), log.CurrentIndex())
}

func writeRecords(t *testing.T, dir string, threshold int, count uint64) {
	t.Helper()

	log, err := NewWAL(Config{Dir: dir, Prefix: "log_", SegmentThreshold: threshold, IsInSyncDiskMode: true})
	require.NoError(t, err)
	for i := uint64(1); i <= count; i++ {
		require.NoError(t, log.Write(NewRecord(i, "key", []byte("value"))))
	}
	require.NoError(t, log.Close())
}

func appendToSegment(t *testing.T, path string, data []byte) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}
