package gowal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBatchDuplicateIndexes(t *testing.T) {
	_, err := NewBatch(
		Record{Index: 1, Key: "key1", Value: []byte("v1")},
		Record{Index: 1, Key: "key1-dup", Value: []byte("v1-dup")},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate index", "Expected error for duplicate index")
}

func TestBatchCopiesRecords(t *testing.T) {
	value := []byte("value")
	batch, err := NewBatch(Record{Index: 1, Key: "key", Value: value})
	require.NoError(t, err)

	value[0] = 'X'
	records := batch.Records()
	require.Equal(t, []byte("value"), records[0].Value)

	records[0].Value[0] = 'Y'
	require.Equal(t, []byte("value"), batch.Records()[0].Value)
}

func TestNewBatchSortsRecordsByIndex(t *testing.T) {
	batch, err := NewBatch(
		Record{Index: 3, Key: "key3", Value: []byte("value3")},
		Record{Index: 1, Key: "key1", Value: []byte("value1")},
		Record{Index: 2, Key: "key2", Value: []byte("value2")},
	)
	require.NoError(t, err)

	records := batch.Records()
	require.Equal(t, uint64(1), records[0].Index)
	require.Equal(t, uint64(2), records[1].Index)
	require.Equal(t, uint64(3), records[2].Index)
}

func TestBatchValidateSequenceAfter(t *testing.T) {
	batch, err := NewBatch(
		Record{Index: 3, Key: "key3", Value: []byte("value3")},
		Record{Index: 4, Key: "key4", Value: []byte("value4")},
		Record{Index: 5, Key: "key5", Value: []byte("value5")},
	)
	require.NoError(t, err)
	require.NoError(t, batch.validateSequenceAfter(2))
	require.ErrorIs(t, batch.validateSequenceAfter(1), ErrNonSequentialIndex)
}

func TestNewBatchRejectsGaps(t *testing.T) {
	_, err := NewBatch(
		Record{Index: 1, Key: "key1", Value: []byte("value1")},
		Record{Index: 3, Key: "key3", Value: []byte("value3")},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNonSequentialIndex)
	require.Contains(t, err.Error(), "key1")
	require.Contains(t, err.Error(), "key3")
}
