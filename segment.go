package gowal

import (
	"bytes"
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

func openSegment(segmentPath string) (*segment, error) {
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open log segment file")
	}

	s := &segment{
		path:  segmentPath,
		file:  file,
		codec: codec{},
	}

	index, lastIndex, entries, err := s.loadIndex()
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

func (s *segment) loadIndex() (map[uint64]Record, uint64, int, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed to seek segment file")
	}

	index := make(map[uint64]Record)
	var lastIndex uint64
	var entries int

	for {
		record, err := s.codec.unmarshal(s.file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, 0, errors.Wrap(err, "failed to decode record from log")
		}

		index[record.Index] = record
		entries++
		if record.Index > lastIndex {
			lastIndex = record.Index
		}
	}

	return index, lastIndex, entries, nil
}
