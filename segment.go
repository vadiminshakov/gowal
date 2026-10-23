package gowal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
)

type segment struct {
	path    string
	file    *os.File
	index   map[uint64]Record
	lastIdx uint64
	entries int
	buf     bytes.Buffer
	codec   codec
}

// openSegment opens the segment file and loads its index.
// If repairTornTail is set, a torn last frame (left by a crash in the middle of a write)
// is truncated instead of failing the load. It must be set only for the last segment.
func openSegment(segmentPath string, repairTornTail bool) (*segment, error) {
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open log segment file")
	}

	s := &segment{
		path:  segmentPath,
		file:  file,
		codec: codec{},
	}

	index, lastIndex, entries, err := s.loadIndex(repairTornTail)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "failed to build index from log segment")
	}

	s.index = index
	s.lastIdx = lastIndex
	s.entries = entries

	return s, nil
}

func (s *segment) Append(records []Record) error {
	s.buf.Reset()
	s.buf.Grow(len(records) * 150)

	for _, record := range records {
		frame, err := s.codec.marshal(record)
		if err != nil {
			return errors.Wrap(err, "failed to marshal record")
		}
		s.buf.Write(frame)
	}

	if _, err := s.file.Write(s.buf.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write record to log")
	}

	for _, record := range records {
		s.index[record.Index] = record
		if record.Index > s.lastIdx {
			s.lastIdx = record.Index
		}
	}
	s.entries += len(records)

	return nil
}

func (s *segment) Sync() error {
	return s.file.Sync()
}

func (s *segment) Close() error {
	return s.file.Close()
}

func (s *segment) Len() int {
	return len(s.index)
}

func (s *segment) Entries() int {
	return s.entries
}

func (s *segment) LastIndex() uint64 {
	return s.lastIdx
}

func (s *segment) Record(index uint64) (Record, bool) {
	record, ok := s.index[index]
	return record.clone(), ok
}

func (s *segment) Path() string {
	return s.path
}

func (s *segment) loadIndex(repairTornTail bool) (map[uint64]Record, uint64, int, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed to seek segment file")
	}

	index := make(map[uint64]Record)
	var lastIndex uint64
	var entries int

	reader := &countingReader{r: bufio.NewReader(s.file)}
	var validSize int64

	for {
		record, err := s.codec.unmarshal(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			if !repairTornTail {
				return nil, 0, 0, errors.Wrapf(err, "failed to decode record from log at offset %d", validSize)
			}
			if err := s.truncateTornTail(validSize, err); err != nil {
				return nil, 0, 0, err
			}
			break
		}

		index[record.Index] = record
		entries++
		if record.Index > lastIndex {
			lastIndex = record.Index
		}
		validSize = reader.n
	}

	return index, lastIndex, entries, nil
}

// truncateTornTail cuts the file at validSize if the bytes after it are a torn last frame,
// otherwise it returns decodeErr as a corruption error.
func (s *segment) truncateTornTail(validSize int64, decodeErr error) error {
	info, err := s.file.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to stat segment file")
	}

	torn, err := isTornTail(s.file, validSize, info.Size(), decodeErr)
	if err != nil {
		return err
	}
	if !torn {
		return errors.Wrapf(decodeErr, "failed to decode record from log at offset %d", validSize)
	}

	if err := s.file.Truncate(validSize); err != nil {
		return errors.Wrap(err, "failed to truncate torn tail")
	}
	if err := s.file.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync truncated segment")
	}

	return nil
}

// isTornTail reports whether the bytes [validSize, size) are a torn last frame:
// an incomplete frame, a zero-filled tail, or a broken frame that ends exactly at EOF.
func isTornTail(file io.ReaderAt, validSize, size int64, decodeErr error) (bool, error) {
	if errors.Is(decodeErr, errPartialFrame) {
		return true, nil
	}

	tail := make([]byte, size-validSize)
	if _, err := file.ReadAt(tail, validSize); err != nil {
		return false, errors.Wrap(err, "failed to read segment tail")
	}

	if bytes.Count(tail, []byte{0}) == len(tail) {
		return true, nil
	}

	if errors.Is(decodeErr, errChecksumMismatch) || errors.Is(decodeErr, errInvalidTrailer) {
		dataLen := int64(binary.BigEndian.Uint32(tail[4:8]))
		return int64(frameHeaderSize)+dataLen+int64(frameTrailerSize) == int64(len(tail)), nil
	}

	return false, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
