package gowal

import (
	"bytes"
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

// ErrRecordTooLarge is returned when an encoded record or batch exceeds 16 MiB.
var ErrRecordTooLarge = errors.New("record too large")

var (
	errPartialFrame     = errors.New("partial frame")
	errChecksumMismatch = errors.New("checksum mismatch")
	errInvalidTrailer   = errors.New("invalid frame trailer")
	errEmptyFrame       = errors.New("frame contains no records")
)

type codec struct{}

func (codec) marshal(records []Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, errEmptyFrame
	}

	data, err := msgpack.Marshal(records)
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

func (codec) unmarshal(r io.Reader) ([]Record, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("%w header", errPartialFrame)
		}
		return nil, err
	}

	wantCRC := binary.BigEndian.Uint32(hdr[0:4])
	dataLen := binary.BigEndian.Uint32(hdr[4:8])
	if dataLen > maxFrameDataSize {
		return nil, fmt.Errorf("frame data too large: %d > %d", dataLen, maxFrameDataSize)
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		// ReadFull returns io.EOF if it read nothing. The header is already read,
		// so the file ends right after the header: this is a partial frame, not the end of the log.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("%w data", errPartialFrame)
		}
		return nil, err
	}

	var trail [frameTrailerSize]byte
	if _, err := io.ReadFull(r, trail[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("%w trailer", errPartialFrame)
		}
		return nil, err
	}

	if binary.BigEndian.Uint32(trail[:]) != frameTrailer {
		return nil, errInvalidTrailer
	}

	if crc32.ChecksumIEEE(data) != wantCRC {
		return nil, errChecksumMismatch
	}

	payload := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(payload)
	msgpackHeaderRecordCount, err := decoder.DecodeArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to decode frame record count: %w", err)
	}
	if msgpackHeaderRecordCount <= 0 {
		return nil, errEmptyFrame
	}

	if msgpackHeaderRecordCount > payload.Len() {
		return nil, fmt.Errorf("invalid frame record count: %d", msgpackHeaderRecordCount)
	}
	records := make([]Record, msgpackHeaderRecordCount)
	for i := range records {
		if err := decoder.Decode(&records[i]); err != nil {
			return nil, fmt.Errorf("failed to decode frame record %d: %w", i, err)
		}
	}
	if payload.Len() != 0 {
		return nil, errors.New("unexpected trailing frame data")
	}

	return records, nil
}
