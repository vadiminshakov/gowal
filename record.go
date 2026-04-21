package gowal

import (
	"bytes"
	"fmt"
	"hash/crc32"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

type Record struct {
	Index uint64
	Key string
=	Value []byte

	checksum uint32
}

type recordWire struct {
	Idx      uint64
	Key      string
	Value    []byte
	Checksum uint32
}

func newRecord(index uint64, key string, value []byte) Record {
	r := Record{Index: index, Key: key, Value: bytes.Clone(value)}
	r.checksum = r.calculateChecksum()
	return r
}

func newTombstone(existing Record) Record {
	return newRecord(existing.Index, existing.Key, []byte("tombstone"))
}

func (r Record) clone() Record {
	r.Value = bytes.Clone(r.Value)
	return r
}

func (r Record) sameRecord(other Record) bool {
	return r.Index == other.Index &&
		r.Key == other.Key &&
		r.checksum == other.checksum &&
		bytes.Equal(r.Value, other.Value)
}

// calculateChecksum calculates CRC32 checksum for the record.
func (r Record) calculateChecksum() uint32 {
	h := crc32.NewIEEE()

	// Write index as 8 bytes
	indexBytes := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		indexBytes[i] = byte(r.Index >> (i * 8))
	}
	h.Write(indexBytes)

	// Write key
	h.Write([]byte(r.Key))

	// Write value
	h.Write(r.Value)

	return h.Sum32()
}

// verifyChecksum verifies that the checksum in the record matches the calculated checksum.
func (r Record) verifyChecksum() error {
	expected := Record{Index: r.Index, Key: r.Key, Value: r.Value}.calculateChecksum()
	if r.checksum != expected {
		return fmt.Errorf("checksum mismatch for index %d: expected %x, got %x", r.Index, expected, r.checksum)
	}
	return nil
}

func (r Record) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(recordWire{
		Idx:      r.Index,
		Key:      r.Key,
		Value:    r.Value,
		Checksum: r.checksum,
	})
}

func (r *Record) DecodeMsgpack(dec *msgpack.Decoder) error {
	var wire recordWire
	if err := dec.Decode(&wire); err != nil {
		return err
	}

	*r = Record{
		Index:    wire.Idx,
		Key:      wire.Key,
		Value:    bytes.Clone(wire.Value),
		checksum: wire.Checksum,
	}

	return nil
}
