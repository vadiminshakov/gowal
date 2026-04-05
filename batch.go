package gowal

import "fmt"

// Batch is a collection of records to be written atomically.
type Batch struct {
	records []Record
}

// NewBatch creates a new Batch from the given records.
func NewBatch(records ...Record) (Batch, error) {
	b := Batch{records: records}
	if err := b.validate(); err != nil {
		return Batch{}, err
	}

	return b, nil
}

// Records returns the records in the batch.
func (b Batch) Records() []Record {
	return b.records
}

// Len returns the number of records in the batch.
func (b Batch) Len() int {
	return len(b.records)
}

// validate checks that there are no duplicate indexes within the batch.
func (b Batch) validate() error {
	indexes := make(map[uint64]struct{}, len(b.records))
	for _, record := range b.records {
		if _, exists := indexes[record.Index]; exists {
			return fmt.Errorf("duplicate index %d ", record.Index)
		}

		indexes[record.Index] = struct{}{}
	}
	return nil
}
