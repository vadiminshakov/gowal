package gowal

import "bytes"

type Record struct {
	Index     uint64
	Key       string
	Value     []byte
	Tombstone bool
}

func NewRecord(index uint64, key string, value []byte) Record {
	return Record{Index: index, Key: key, Value: bytes.Clone(value)}
}

func newTombstone(existing Record) Record {
	return Record{
		Index:     existing.Index,
		Key:       existing.Key,
		Tombstone: true,
	}
}

func (r Record) clone() Record {
	r.Value = bytes.Clone(r.Value)
	return r
}
