package gowal

import (
	"fmt"
	"hash/crc32"
)

type msg struct {
	Idx      uint64
	Key      string
	Value    []byte
	Checksum uint32
}

func (m msg) Index() uint64 {
	return m.Idx
}

// calculateChecksum calculates CRC32 checksum for the message
func (m msg) calculateChecksum() uint32 {
	h := crc32.NewIEEE()

	// Write index as 8 bytes
	indexBytes := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		indexBytes[i] = byte(m.Idx >> (i * 8))
	}
	h.Write(indexBytes)

	// Write key
	h.Write([]byte(m.Key))

	// Write value
	h.Write(m.Value)

	return h.Sum32()
}

// verifyChecksum verifies that the checksum in the message matches the calculated checksum
func (m msg) verifyChecksum() error {
	expected := msg{Idx: m.Idx, Key: m.Key, Value: m.Value}.calculateChecksum()
	if m.Checksum != expected {
		return fmt.Errorf("checksum mismatch for index %d: expected %x, got %x", m.Idx, expected, m.Checksum)
	}
	return nil
}
