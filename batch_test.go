package gowal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchValidateDuplicateIndexes(t *testing.T) {
	batch := NewBatch(
		NewRecord(1, "key1", []byte("v1")),
		NewRecord(1, "key1-dup", []byte("v1-dup")),
	)
	err := batch.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate index", "Expected error for duplicate index")
}

func TestBatchCopiesRecords(t *testing.T) {
	value := []byte("value")
	batch := NewBatch(NewRecord(1, "key", value))

	value[0] = 'X'
	records := batch.Records()
	require.Equal(t, []byte("value"), records[0].Value)

	records[0].Value[0] = 'Y'
	require.Equal(t, []byte("value"), batch.Records()[0].Value)
}

func TestBatchSortsRecordsByIndex(t *testing.T) {
	batch := NewBatch(
		NewRecord(3, "key3", []byte("value3")),
		NewRecord(1, "key1", []byte("value1")),
		NewRecord(2, "key2", []byte("value2")),
	)
	batch.sort()

	records := batch.Records()
	require.Equal(t, uint64(1), records[0].Index)
	require.Equal(t, uint64(2), records[1].Index)
	require.Equal(t, uint64(3), records[2].Index)
}

func TestBatchValidateSequenceAfter(t *testing.T) {
	batch := NewBatch(
		NewRecord(3, "key3", []byte("value3")),
		NewRecord(4, "key4", []byte("value4")),
		NewRecord(5, "key5", []byte("value5")),
	)
	require.NoError(t, batch.validateSequenceAfter(2))
	require.ErrorIs(t, batch.validateSequenceAfter(1), ErrNonSequentialIndex)
}

func TestBatchValidateRejectsGaps(t *testing.T) {
	batch := NewBatch(
		NewRecord(1, "key1", []byte("value1")),
		NewRecord(3, "key3", []byte("value3")),
	)
	err := batch.validate()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNonSequentialIndex)
	require.Contains(t, err.Error(), "key1")
	require.Contains(t, err.Error(), "key3")
}
