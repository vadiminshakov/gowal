package gowal

import (
	"cmp"
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
