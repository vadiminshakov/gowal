package gowal

import (
	"bytes"
	"fmt"
	"slices"
)

// Batch is a collection of records to be written atomically.
type Batch struct {
	records                []Record
	createdWithConstructor bool
}

// NewBatch creates a new Batch from the given records.
func NewBatch(records ...Record) (Batch, error) {
	b := Batch{records: cloneRecords(records), createdWithConstructor: true}
	b.sort()
	if err := b.validate(); err != nil {
		return Batch{}, err
	}

	return b, nil
}

// Records returns the records in the batch.
func (b Batch) Records() []Record {
	return cloneRecords(b.records)
}

// Len returns the number of records in the batch.
func (b Batch) Len() int {
	return len(b.records)
}

func (b Batch) validateSequenceAfter(lastIndex uint64) error {
	if len(b.records) == 0 {
		return nil
	}

	expectedIndex := lastIndex + 1
	if b.records[0].Index != expectedIndex {
		return fmt.Errorf("%w: expected index %d, got %d", ErrNonSequentialIndex, expectedIndex, b.records[0].Index)
	}
	return nil
}

func (b Batch) sort() {
	slices.SortFunc(b.records, func(a, b Record) int {
		if a.Index < b.Index {
			return -1
		}
		if a.Index > b.Index {
			return 1
		}
		return 0
	})
}

func cloneRecords(records []Record) []Record {
	cloned := slices.Clone(records)
	for i := range cloned {
		cloned[i].Value = bytes.Clone(cloned[i].Value)
	}
	return cloned
}

// validate checks that indexes inside the batch are contiguous.
func (b Batch) validate() error {
	for i := 1; i < len(b.records); i++ {
		expectedIndex := b.records[i-1].Index + 1
		if b.records[i].Index != expectedIndex {
			if b.records[i].Index == b.records[i-1].Index {
				return fmt.Errorf("duplicate index %d ", b.records[i].Index)
			}
			return fmt.Errorf(
				"%w between keys %q and %q: expected index %d after index %d, got %d",
				ErrNonSequentialIndex,
				b.records[i-1].Key,
				b.records[i].Key,
				expectedIndex,
				b.records[i-1].Index,
				b.records[i].Index,
			)
		}
	}
	return nil
}
