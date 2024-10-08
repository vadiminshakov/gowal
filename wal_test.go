package gowal

import (
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
)

func TestSetGet(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	for i := 0; i < 10; i++ {
		key, value, ok := log.Get(uint64(i))
		require.True(t, ok)
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
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	iter := log.Iterator()
	for i := 0; i < 10; i++ {
		msg, ok := iter()
		require.True(t, ok)
		require.Equal(t, "key"+strconv.Itoa(i), msg.Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(msg.Value))
	}

	_, ok := iter()
	require.False(t, ok)

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestLoadIndexMsg(t *testing.T) {
	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	stat, err := log.log.Stat()
	require.NoError(t, err)

	index, err := loadIndexes(log.log, stat)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.Equal(t, "key"+strconv.Itoa(i), index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestSegmentRotationForMsgs(t *testing.T) {
	segmentThreshold := 10

	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: segmentThreshold,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	segmentsNumber := 6
	// here we exceed the segment size threshold (segmentThreshold), create new segment, keep old segment on disk until tmpIndexBufferThreshold
	// is reached, then del old segment and write 10 more log
	for i := 0; i < segmentThreshold*segmentsNumber+10; i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	stat, err := log.log.Stat()
	require.NoError(t, err)

	// now we have only one segment on disk with tmpIndexBufferThreshold+10 log,
	// load it in the memory
	index, err := loadIndexes(log.log, stat)
	require.NoError(t, err)

	// check all saved log in the index
	// we have all log with height greater than segmentThreshold and less than segmentThreshold+(tmpIndexBufferThreshold+10)
	for i := segmentThreshold * segmentsNumber; i < segmentThreshold*segmentsNumber+10; i++ {
		require.Equal(t, "key"+strconv.Itoa(i), index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

// create two segments, app down, up and repair index.
func TestServiceDownUpAndRepairIndex(t *testing.T) {
	segmentThreshold := 10

	log, err := NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 0; i < segmentThreshold+(segmentThreshold/2); i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	require.NoError(t, log.Close())

	log, err = NewWAL(Config{
		Dir:              "./testlogdata",
		Prefix:           "log_",
		SegmentThreshold: 10,
		MaxSegments:      5,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)

	for i := 0; i < segmentThreshold+(segmentThreshold/2); i++ {
		require.Equal(t, "key"+strconv.Itoa(i), log.index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(log.index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}
