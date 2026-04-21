package gowal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentAppendAddsRecordsToIndex(t *testing.T) {
	dir := t.TempDir()

	seg, err := openSegment(segmentPath(dir, "log_", 0))
	require.NoError(t, err)
	defer seg.Close()

	require.Equal(t, segmentPath(dir, "log_", 0), seg.Path())

	record := newRecord(1, "key1", []byte("value1"))

	require.NoError(t, seg.Append([]Record{record}))

	got, ok := seg.Record(1)
	require.True(t, ok)
	require.Equal(t, record, got)
	require.Equal(t, 1, seg.Len())
}

func TestOpenSegmentLoadsIndexFromExistingFile(t *testing.T) {
	dir := t.TempDir()

	seg, err := openSegment(segmentPath(dir, "log_", 0))
	require.NoError(t, err)

	record := newRecord(1, "key1", []byte("value1"))

	require.NoError(t, seg.Append([]Record{record}))
	require.NoError(t, seg.Close())

	loadedSegment, err := openSegment(segmentPath(dir, "log_", 0))
	require.NoError(t, err)
	defer loadedSegment.Close()

	got, ok := loadedSegment.Record(1)
	require.True(t, ok)
	require.Equal(t, record, got)
}

func TestSegmentLoadCorruptedSegmentReturnsError(t *testing.T) {
	dir := t.TempDir()

	seg, err := openSegment(segmentPath(dir, "log_", 0))
	require.NoError(t, err)

	record := newRecord(1, "key1", []byte("value1"))

	require.NoError(t, seg.Append([]Record{record}))
	require.NoError(t, seg.Close())

	file, err := os.OpenFile(seg.Path(), os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = file.Write([]byte("corrupted data"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, err = openSegment(seg.Path())
	require.Error(t, err)
}

func TestSegmentNamerParsesConfiguredPrefix(t *testing.T) {
	namer := segmentNamer{dir: t.TempDir(), prefix: "wal_segment_"}

	number, ok, err := namer.parse("wal_segment_42")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, segmentNumber(42), number)

	_, ok, err = namer.parse("other_42")
	require.NoError(t, err)
	require.False(t, ok)

	_, _, err = namer.parse("wal_segment_bad")
	require.Error(t, err)
}
