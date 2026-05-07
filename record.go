package gowal

import "bytes"

type Record struct {
	Index uint64
	Key   string
	Value []byte
}

func NewRecord(index uint64, key string, value []byte) Record {
	return Record{Index: index, Key: key, Value: bytes.Clone(value)}
}

func newTombstone(existing Record) Record {
	return NewRecord(existing.Index, existing.Key, []byte("tombstone"))
}

func (r Record) clone() Record {
	r.Value = bytes.Clone(r.Value)
	return r
}

func (r Record) sameRecord(other Record) bool {
	return r.Index == other.Index &&
		r.Key == other.Key &&
		bytes.Equal(r.Value, other.Value)
}
