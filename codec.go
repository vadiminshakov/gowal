package gowal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	frameTrailer     uint32 = 0xDEADC0DE
	frameHeaderSize  int    = 8 // CRC32(4) + Length(4)
	frameTrailerSize int    = 4
)

type codec struct{}

func (codec) marshal(r Record) ([]byte, error) {
	data, err := msgpack.Marshal(r)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, frameHeaderSize+len(data)+frameTrailerSize)
	binary.BigEndian.PutUint32(frame[0:4], crc32.ChecksumIEEE(data))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(data)))
	copy(frame[8:], data)
	binary.BigEndian.PutUint32(frame[8+len(data):], frameTrailer)

	return frame, nil
}

func (codec) unmarshal(r io.Reader) (Record, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("partial frame header")
		}
		return Record{}, err
	}

	wantCRC := binary.BigEndian.Uint32(hdr[0:4])
	dataLen := binary.BigEndian.Uint32(hdr[4:8])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		if err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("partial frame data")
		}
		return Record{}, err
	}

	var trail [frameTrailerSize]byte
	if _, err := io.ReadFull(r, trail[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("partial frame trailer")
		}
		return Record{}, err
	}

	if binary.BigEndian.Uint32(trail[:]) != frameTrailer {
		return Record{}, fmt.Errorf("invalid frame trailer")
	}

	if crc32.ChecksumIEEE(data) != wantCRC {
		return Record{}, fmt.Errorf("checksum mismatch")
	}

	var rec Record
	if err := msgpack.Unmarshal(data, &rec); err != nil {
		return Record{}, err
	}

	return rec, nil
}
