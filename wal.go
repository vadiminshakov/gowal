package gowal

import (
	"bytes"
	"encoding/gob"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal/msg"
	"os"
	"path"
	"sort"
	"strconv"
)

const (
	segmentThreshold = 1000
	maxSegments      = 5

	isInSyncDiskMode = false
)

var ErrExists = errors.New("msg with such index already exists")

// Wal is used to log on disk.
//
// Log is append-only, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
// Log is divided into two parts: log log and votes log. Each part has its own index, which is used to find record by its height.
// Index is stored in memory and loaded from disk on Wal init.
//
// This code is intentionally monomorphized for log and votes, generics can slow the app and make code more complicated.
type Wal struct {
	// append-only log with proposed messages that node consumed
	log *os.File

	// index that matches height of msg record with offset in file
	index    map[uint64]msg.Msg
	tmpIndex map[uint64]msg.Msg

	// gob encoder for proposed messages
	enc *gob.Encoder

	// buffer for proposed messages
	buf *bytes.Buffer

	// path to directory with logs
	pathToLogsDir string

	// name of the old segment
	oldestSegName string

	// offset of last record in file
	lastOffset int64

	// number of segments for log log
	segmentsNumber int

	// prefix for segment files
	prefix string
}

func NewWAL(dir string, prefix string) (*Wal, error) {
	segmentsNumbers, err := findSegmentNumber(dir, prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	// load them segments into mem
	fd, stat, index, err := segmentInfoAndIndex(segmentsNumbers, path.Join(dir, prefix))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load log segments")
	}
	numberOfSegments := len(index) / segmentThreshold
	if numberOfSegments == 0 {
		numberOfSegments = 1
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	return &Wal{log: fd, index: index, tmpIndex: make(map[uint64]msg.Msg),
		buf: &buf, enc: enc, lastOffset: stat.Size(), pathToLogsDir: dir,
		segmentsNumber: numberOfSegments, prefix: prefix}, nil
}

// Set writes key/value pair to the log log.
func (c *Wal) Set(index uint64, key string, value []byte) error {
	if _, ok := c.index[index]; ok {
		return ErrExists
	}

	// rotate segment if threshold is reached
	// (close current segment, open new one with incremented suffix in name)
	if len(c.index) == segmentThreshold*c.segmentsNumber {
		c.buf.Reset()
		c.enc = gob.NewEncoder(c.buf)
		if err := c.log.Close(); err != nil {
			return errors.Wrap(err, "failed to close log log file")
		}

		c.oldestSegName = c.oldestSegmentName(c.segmentsNumber)

		segmentNumber, err := extractSegmentNum(c.log.Name())
		if err != nil {
			return errors.Wrap(err, "failed to extract segment number from log log file name")
		}

		segmentNumber++
		c.log, err = os.OpenFile(path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(segmentNumber)), os.O_RDWR|os.O_CREATE, 0755)
		c.segmentsNumber = segmentNumber + 1

		c.lastOffset = 0
	}

	// gob encode key and value
	if err := c.enc.Encode(msg.Msg{Key: key, Value: value, Idx: index}); err != nil {
		return errors.Wrap(err, "failed to encode msg for log")
	}
	// write to log at last offset
	_, err := c.log.WriteAt(c.buf.Bytes(), c.lastOffset)
	if err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}

	if isInSyncDiskMode {
		if err := c.log.Sync(); err != nil {
			return errors.Wrap(err, "failed to sync msg log file")
		}
	}

	c.lastOffset += int64(c.buf.Len())
	c.buf.Reset()

	// update index
	c.index[index] = msg.Msg{Key: key, Value: value, Idx: index}

	c.rotateSegments(msg.Msg{Key: key, Value: value, Idx: index})

	return nil
}

// oldestSegmentName returns name of the oldest segment in the directory.
func (c *Wal) oldestSegmentName(numberOfSegments int) string {
	latestSegmentIndex := 0
	if numberOfSegments >= maxSegments {
		latestSegmentIndex = numberOfSegments - maxSegments
	}
	return path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(int(latestSegmentIndex)))
}

// Get queries value at specific index in the log log.
func (c *Wal) Get(index uint64) (string, []byte, bool) {
	msg, ok := c.index[index]
	if !ok {
		return "", nil, false
	}

	return msg.Key, msg.Value, true
}

// Iterator returns iterator for the WAL messages.
// It returns function that returns next message and bool flag,
// which is false if there are no more messages.
// Messages are returned from the oldest to the newest.
func (c *Wal) Iterator() func() (msg.Msg, bool) {
	indexes := make([]uint64, 0, len(c.index))

	for k := range c.index {
		indexes = append(indexes, k)
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})

	i := 0

	return func() (msg.Msg, bool) {
		if i >= len(indexes) {
			return msg.Msg{}, false
		}

		msg := c.index[indexes[i]]
		i++

		return msg, true
	}
}

// CurrentIndex returns current index of the log.
func (c *Wal) CurrentIndex() uint64 {
	return uint64(len(c.index))
}

// Close closes log files.
func (c *Wal) Close() error {
	if err := c.log.Close(); err != nil {
		return errors.Wrap(err, "failed to close log log file")
	}
	return nil
}
