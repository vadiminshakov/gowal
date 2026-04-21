package gowal

import (
	"bytes"
	"io"
	"os"

	"github.com/pkg/errors"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type segment struct {
	path    string
	file    *os.File
	index   map[uint64]Record
	lastIdx uint64
	buf     bytes.Buffer
	encoder *msgpack.Encoder
}

func openSegment(segmentPath string) (*segment, error) {
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open log segment file")
	}

	index, lastIndex, err := loadSegmentIndex(file)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "failed to build index from log segment")
	}

	s := &segment{
		path:    segmentPath,
		file:    file,
		index:   index,
		lastIdx: lastIndex,
	}
	s.encoder = msgpack.NewEncoder(&s.buf)

	return s, nil
}

func (s *segment) Append(records []Record) error {
	s.buf.Reset()
	s.buf.Grow(len(records) * 128) // small heuristic; avoids repeated growth on small/medium batches

	for _, record := range records {
		if err := s.encoder.Encode(record); err != nil {
			return errors.Wrap(err, "failed to encode record")
		}
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

func loadSegmentIndex(file *os.File) (map[uint64]Record, uint64, error) {
	file.Seek(0, io.SeekStart)

	index := make(map[uint64]Record)
	var lastIndex uint64
	dec := msgpack.NewDecoder(file)

	for {
		var record Record
		if err := dec.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, errors.Wrap(err, "failed to decode indexed record from log")
		}

		// verify checksum for each loaded record
		if err := record.verifyChecksum(); err != nil {
			return nil, 0, errors.Wrapf(err, "corrupted record at index %d", record.Index)
		}

		index[record.Index] = record
		if record.Index > lastIndex {
			lastIndex = record.Index
		}
	}

	return index, lastIndex, nil
}
