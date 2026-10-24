package gowal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	frameTrailer     uint32 = 0xDEADC0DE
	frameHeaderSize  int    = 8 // CRC32(4) + Length(4)
	frameTrailerSize int    = 4
	maxFrameDataSize uint32 = 16 * 1024 * 1024 // 16 MiB
)

// ErrRecordTooLarge is returned when an encoded record exceeds the maximum frame size.
var ErrRecordTooLarge = errors.New("record too large")

var (
	errPartialFrame     = errors.New("partial frame")
	errChecksumMismatch = errors.New("checksum mismatch")
	errInvalidTrailer   = errors.New("invalid frame trailer")
)

type codec struct{}

func (codec) marshal(r Record) ([]byte, error) {
	data, err := msgpack.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(data) > int(maxFrameDataSize) {
		return nil, fmt.Errorf("%w: frame data too large: %d > %d", ErrRecordTooLarge, len(data), maxFrameDataSize)
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
			return Record{}, fmt.Errorf("%w header", errPartialFrame)
		}
		return Record{}, err
	}

	wantCRC := binary.BigEndian.Uint32(hdr[0:4])
	dataLen := binary.BigEndian.Uint32(hdr[4:8])
	if dataLen > maxFrameDataSize {
		return Record{}, fmt.Errorf("frame data too large: %d > %d", dataLen, maxFrameDataSize)
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		// ReadFull returns io.EOF if it read nothing. The header is already read,
		// so the file ends right after the header: this is a partial frame, not the end of the log.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("%w data", errPartialFrame)
		}
		return Record{}, err
	}

	var trail [frameTrailerSize]byte
	if _, err := io.ReadFull(r, trail[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("%w trailer", errPartialFrame)
		}
		return Record{}, err
	}

	if binary.BigEndian.Uint32(trail[:]) != frameTrailer {
		return Record{}, errInvalidTrailer
	}

	if crc32.ChecksumIEEE(data) != wantCRC {
		return Record{}, errChecksumMismatch
	}

	var rec Record
	if err := msgpack.Unmarshal(data, &rec); err != nil {
		return Record{}, err
	}

	return rec, nil
}
