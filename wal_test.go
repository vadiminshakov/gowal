package gowal

import (
	"cmp"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

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

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	for i := 0; i < 10; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), key)
		require.Equal(t, "value"+strconv.Itoa(i), string(value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
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

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	t.Run("Iterator", func(t *testing.T) {
		i := 0
		for m := range log.Iterator() {
			require.Equal(t, "key"+strconv.Itoa(i), m.Key)
			require.Equal(t, "value"+strconv.Itoa(i), string(m.Value))
			i++
		}
	})

	t.Run("PullIterator", func(t *testing.T) {
		iter, stop := log.PullIterator()
		defer stop()

		i := 0
		for {
			msg, ok := iter()
			if !ok {
				break
			}

			require.Equal(t, "key"+strconv.Itoa(i), msg.Key)
			require.Equal(t, "value"+strconv.Itoa(i), string(msg.Value))
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

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	index, err := loadIndexes(log.log)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
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
	for i := 0; i < segmentThreshold*segmentsNumber+10; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	// load index of last segment
	index, err := loadIndexes(log.log)
	require.NoError(t, err)

	// check
	indexValues := slices.Collect(maps.Values(index))

	slices.SortFunc(indexValues, func(a, b msg) int {
		return cmp.Compare(a.Idx, b.Idx)
	})

	for i, v := range indexValues {
		keyWithOffset := i + segmentThreshold*segmentsNumber
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
	for i := 0; i < segmentThreshold; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	// create second segment
	for i := segmentThreshold; i < segmentThreshold*2; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	// close the wal
	require.NoError(t, log.Close())

	// open the wal again
	log, err = initWal()
	require.NoError(t, err)

	// check
	for i := range segmentThreshold * 2 {
		require.Equal(t, "key"+strconv.Itoa(i), log.index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(log.index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
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

	for i := 0; i < 2; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	// verify data can be read correctly (checksums verified on read)
	for i := 0; i < 2; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), key)
		require.Equal(t, []byte("value"+strconv.Itoa(i)), value)
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

	for i := 0; i < 2; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
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

	for i := 0; i < 5; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	// verify all data can be read correctly (checksums verified on read)
	for i := 0; i < 5; i++ {
		key, value, err := log.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, "key"+strconv.Itoa(i), key)
		require.Equal(t, []byte("value"+strconv.Itoa(i)), value)
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

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Write(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	// close and corrupt the data by writing some garbage to the segment file
	log.Close()
	f, err := os.OpenFile("./testlogdata/log_4", os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	f.Write([]byte("corrupted data"))
	f.Close()

	// recover - should remove corrupted segment
	removedFiles, err := UnsafeRecover("./testlogdata", "log_")
	require.NoError(t, err)
	require.Equal(t, 1, len(removedFiles))
	require.ElementsMatch(t, []string{"testlogdata/log_4"}, removedFiles)

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
		key, value, err := log.Get(999)
		require.NoError(t, err)
		require.Equal(t, "", key)
		require.Nil(t, value)
	})

	t.Run("WriteTombstone for existing record", func(t *testing.T) {
		// write a record
		err := log.Write(1, "key1", []byte("value1"))
		require.NoError(t, err)

		// verify record exists
		key, value, err := log.Get(1)
		require.NoError(t, err)
		require.Equal(t, "key1", key)
		require.Equal(t, "value1", string(value))

		// write tombstone
		err = log.WriteTombstone(1)
		require.NoError(t, err)

		// verify tombstone exists with same key
		key, value, err = log.Get(1)
		require.NoError(t, err)
		require.Equal(t, "key1", key)
		require.Equal(t, "tombstone", string(value))
	})

	t.Run("WriteTombstone appears in iterator", func(t *testing.T) {
		// write some records
		err := log.Write(3, "key3", []byte("value3"))
		require.NoError(t, err)
		err = log.Write(4, "key4", []byte("value4"))
		require.NoError(t, err)

		// write tombstone for one of them
		err = log.WriteTombstone(3)
		require.NoError(t, err)

		// check iterator contains tombstone
		found := false
		for msg := range log.Iterator() {
			if msg.Idx == 3 {
				require.Equal(t, "key3", msg.Key)
				require.Equal(t, "tombstone", string(msg.Value))
				found = true
			}
		}
		require.True(t, found, "Tombstone should appear in iterator")
	})

	t.Run("WriteTombstone overwrites record from main index", func(t *testing.T) {
		// simulate having a record in main index by manually adding it
		// (this simulates the case where record was persisted to main index)
		m := msg{Key: "key5", Value: []byte("value5"), Idx: 5}
		m.Checksum = m.calculateChecksum()
		log.mu.Lock()
		log.index[5] = m
		log.mu.Unlock()

		// verify record exists in main index
		key, value, err := log.Get(5)
		require.NoError(t, err)
		require.Equal(t, "key5", key)
		require.Equal(t, "value5", string(value))

		// write tombstone
		err = log.WriteTombstone(5)
		require.NoError(t, err)

		// verify tombstone overwrites the record
		key, value, err = log.Get(5)
		require.NoError(t, err)
		require.Equal(t, "key5", key)
		require.Equal(t, "tombstone", string(value))

		// verify record is removed from main index
		log.mu.RLock()
		_, existsInMainIndex := log.index[5]
		_, existsInTmpIndex := log.tmpIndex[5]
		log.mu.RUnlock()

		require.False(t, existsInMainIndex, "Record should be removed from main index")
		require.True(t, existsInTmpIndex, "Tombstone should exist in temp index")
	})
}

func TestWriteBatch_DuplicateIndexesInBatch(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	batch := []Record{
		{Index: 1, Key: "key1", Value: []byte("v1")},
		{Index: 1, Key: "key1-dup", Value: []byte("v1-dup")},
	}

	err = wal.WriteBatch(batch)
	require.ErrorIs(t, err, ErrExists, "expected ErrExists for duplicate indexes in batch")
}

func TestWriteBatch_DuplicateWithExistingIndex(t *testing.T) {
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
	require.NoError(t, wal.Write(5, "initial", []byte("value")))

	batch := []Record{
		{Index: 5, Key: "key5-conflict", Value: []byte("valueB")},
		{Index: 6, Key: "key6", Value: []byte("value6")},
	}

	err = wal.WriteBatch(batch)
	require.ErrorIs(t, err, ErrExists, "expected ErrExists for index conflict with existing WAL entry")
}

func TestWriteBatch_AcrossSegmentThreshold(t *testing.T) {
	dir := t.TempDir()
	segmentThreshold := 3

	wal, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: segmentThreshold,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	defer wal.Close()

	// Build a batch exactly at threshold boundary
	batch := make([]Record, segmentThreshold+1)
	for i := 0; i < len(batch); i++ {
		batch[i] = Record{
			Index: uint64(i + 1),
			Key:   fmt.Sprintf("key%d", i+1),
			Value: []byte(fmt.Sprintf("value%d", i+1)),
		}
	}

	require.NoError(t, wal.WriteBatch(batch))

	// All records should be queryable via Get
	for _, r := range batch {
		key, value, err := wal.Get(r.Index)
		require.NoError(t, err, "expected record to exist after batch write and rotation")
		require.Equal(t, r.Key, key)
		require.Equal(t, r.Value, value)
	}

	// CurrentIndex should reflect the highest index
	require.Equal(t, uint64(len(batch)), wal.CurrentIndex())
}

// getRandomData returns a random byte slice of the given size.
func getRandomData(sizeBytes int) []byte {
	if sizeBytes <= 0 {
		return nil
	}
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]byte, sizeBytes)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func benchmarkWALWrite(b *testing.B, batchSize int) {
	dir := b.TempDir()

	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10000,
		MaxSegments:      10000000,
		IsInSyncDiskMode: false,
	})
	require.NoError(b, err)
	defer func() {
		require.NoError(b, log.Close())
	}()

	valueData := getRandomData(1024)

	records := make([]Record, b.N*batchSize)
	for i := range records {
		records[i] = Record{
			Index: uint64(i),
			Value: valueData,
			Key:   "key" + strconv.Itoa(i),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	total := 0

	for i := 0; i < len(records); i += batchSize {
		end := min(i+batchSize, len(records))

		if batchSize == 1 {
			require.NoError(b, log.Write(records[i].Index, records[i].Key, records[i].Value))
			total++
		} else {
			require.NoError(b, log.WriteBatch(records[i:end]))
			total += end - i
		}
	}

	elapsed := time.Since(start)
	b.ReportMetric(float64(total)/elapsed.Seconds(), "records/s")
	b.ReportMetric(elapsed.Seconds(), "sec")
}

func BenchmarkWal_Write(b *testing.B)           { benchmarkWALWrite(b, 1) }
func BenchmarkWal_WriteBatch10(b *testing.B)    { benchmarkWALWrite(b, 10) }
func BenchmarkWal_WriteBatch100(b *testing.B)   { benchmarkWALWrite(b, 100) }
func BenchmarkWal_WriteBatch1000(b *testing.B)  { benchmarkWALWrite(b, 1000) }
func BenchmarkWal_WriteBatch10000(b *testing.B) { benchmarkWALWrite(b, 10000) }

// Batching significantly improves WAL write throughput (~6–7x, from ~265k to ~1.8M records/s)
// by amortizing per-operation overhead.
// Most of the gains are achieved with batch sizes of 100–1000;
// larger batches provide diminishing returns while increasing memory usage and allocations.
//
// Per-record allocations remain roughly constant (~6 allocs, ~1.6–2KB),
// indicating that batching reduces syscall/overhead cost but does not optimize per-record memory behavior.
