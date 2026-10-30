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
}

// openSegment opens the segment file and loads its index.
// If repairTornTail is set, a torn last frame (left by a crash in the middle of a write)
// is truncated instead of failing the load. It must be set only for the last segment.
// seq is shared between the segments of the log, so a gap between segments is detected too.
func openSegment(segmentPath string, repairTornTail bool, seq *indexSequence) (*segment, error) {
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open log segment file")
	}

	s := &segment{
		path: segmentPath,
		file: file,
	}

	index, lastIndex, entries, err := s.loadIndex(repairTornTail, seq)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "failed to build index from log segment")
	}

	s.index = index
	s.lastIdx = lastIndex
	s.entries = entries

	return s, nil
}

// Append writes the pre-encoded frame for records and, if sync is set, syncs it.
func (s *segment) Append(records []Record, frame []byte, sync bool) error {
	if _, err := s.file.Write(frame); err != nil {
		return errors.Wrap(err, "failed to write records to log")
	}

	if sync {
		if err := s.file.Sync(); err != nil {
			return errors.Wrap(err, "failed to sync log")
		}
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

func (s *segment) loadIndex(repairTornTail bool, seq *indexSequence) (map[uint64]Record, uint64, int, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed to seek segment file")
	}

	index := make(map[uint64]Record)
	var lastIndex uint64
	var entries int

	validSize, err := scanFrames(s.file, seq, func(record Record) {
		index[record.Index] = record
		entries++
		if record.Index > lastIndex {
			lastIndex = record.Index
		}
	})
	if err != nil {
		if !repairTornTail || errors.Is(err, ErrIndexGap) {
			return nil, 0, 0, errors.Wrapf(err, "failed to decode record from log at offset %d", validSize)
		}
		if err := s.truncateTornTail(validSize, err); err != nil {
			return nil, 0, 0, err
		}
	}

	return index, lastIndex, entries, nil
}

// scanFrames reads frames from r until EOF, the first broken frame or the first index gap.
// All records in a frame must pass validation before any of them are applied.
// It returns the size of the valid prefix and the error that stopped the scan, or nil at a clean EOF.
func scanFrames(r io.Reader, seq *indexSequence, fn func(Record)) (int64, error) {
	reader := &countingReader{r: bufio.NewReader(r)}
	var validSize int64

	for {
		records, err := codec{}.unmarshal(reader)
		if err == io.EOF {
			return validSize, nil
		}
		if err != nil {
			return validSize, err
		}
		nextSeq := *seq
		for _, record := range records {
			if err := nextSeq.next(record); err != nil {
				return validSize, err
			}
		}

		*seq = nextSeq
		for _, record := range records {
			fn(record)
		}
		validSize = reader.n
	}
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
