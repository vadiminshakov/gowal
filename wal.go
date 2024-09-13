package gowal

import (
	"bytes"
	"encoding/gob"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal/msg"
	"os"
	"path"
	"strconv"
)

const (
	segmentThreshold = 1000
)

var ErrExists = errors.New("msg with such index already exists")

const maxSegments = 5

// Wal is used to store votes and msgs on disk.
//
// Log is append-only, so we can't delete records from it, but log is divided into segments, which are rotated (oldest deleted) when
// segments number threshold is reached.
// Log is divided into two parts: msgs log and votes log. Each part has its own index, which is used to find record by its height.
// Index is stored in memory and loaded from disk on startup.
//
// This code is intentionally monomorphized for msgs and votes, generics can slow the app and make code more complicated.
type Wal struct {
	// append-only log with proposed messages that node consumed
	msgs *os.File

	// index that matches height of msg record with offset in file
	index    map[uint64]msg.Msg
	tmpIndex map[uint64]msg.Msg

	// gob encoder for proposed messages
	enc *gob.Encoder

	// buffer for proposed messages
	buf *bytes.Buffer

	// path to directory with logs
	pathToLogsDir string

	// name of the old segment for msgs log
	oldestMsgsSegmentName string

	// offset of last msg record in file
	lastOffsetMsgs int64

	// number of segments for msgs log
	segmentsNumberMsgs int
}

func NewOnDiskLog(dir string) (*Wal, error) {
	msgSegmentsNumbers, err := findSegmentNumber(dir, "msgs_")
	if err != nil {
		return nil, errors.Wrap(err, "failed to find segment numbers")
	}

	// load them segments into mem
	msgs, statMsgs, msgsIndex, err := segmentInfoAndIndex(msgSegmentsNumbers, path.Join(dir, "msgs_"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load msgs segments")
	}
	numberOfMsgsSegments := len(msgsIndex) / segmentThreshold
	if numberOfMsgsSegments == 0 {
		numberOfMsgsSegments = 1
	}

	var bufMsgs bytes.Buffer
	encMsgs := gob.NewEncoder(&bufMsgs)

	return &Wal{msgs: msgs, index: msgsIndex, tmpIndex: make(map[uint64]msg.Msg),
		buf: &bufMsgs, enc: encMsgs, lastOffsetMsgs: statMsgs.Size(), pathToLogsDir: dir,
		segmentsNumberMsgs: numberOfMsgsSegments}, nil
}

// Set writes key/value pair to the msgs log.
func (c *Wal) Set(index uint64, key string, value []byte) error {
	if _, ok := c.index[index]; ok {
		return ErrExists
	}

	// rotate segment if threshold is reached
	// (close current segment, open new one with incremented suffix in name)
	itemsAddedTotal := len(c.index)
	if itemsAddedTotal > maxSegments*segmentThreshold {
		itemsAddedTotal = itemsAddedTotal + (c.segmentsNumberMsgs-1)*segmentThreshold
	}
	if itemsAddedTotal == segmentThreshold*c.segmentsNumberMsgs {
		c.buf.Reset()
		c.enc = gob.NewEncoder(c.buf)
		if err := c.msgs.Close(); err != nil {
			return errors.Wrap(err, "failed to close msgs log file")
		}

		c.oldestMsgsSegmentName = c.oldestSegmentName(c.segmentsNumberMsgs)

		segmentIndex, err := extractSegmentNum(c.msgs.Name())
		if err != nil {
			return errors.Wrap(err, "failed to extract segment number from msgs log file name")
		}

		segmentIndex++
		c.msgs, err = os.OpenFile(path.Join(c.pathToLogsDir, "msgs_"+strconv.Itoa(segmentIndex)), os.O_RDWR|os.O_CREATE, 0755)
		c.segmentsNumberMsgs = segmentIndex + 1

		c.lastOffsetMsgs = 0
	}

	// gob encode key and value
	if err := c.enc.Encode(msg.Msg{Key: key, Value: value, Idx: index}); err != nil {
		return errors.Wrap(err, "failed to encode msg for log")
	}
	// write to log at last offset
	_, err := c.msgs.WriteAt(c.buf.Bytes(), c.lastOffsetMsgs)
	if err != nil {
		return errors.Wrap(err, "failed to write msg to log")
	}
	//if err := c.msgs.Sync(); err != nil {
	//	return errors.Wrap(err, "failed to sync msg log file")
	//}

	c.lastOffsetMsgs += int64(c.buf.Len())
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
	return path.Join(c.pathToLogsDir, "msgs_"+strconv.Itoa(int(latestSegmentIndex)))
}

// Get queries value at specific index in the msgs log.
func (c *Wal) Get(index uint64) (string, []byte, bool) {
	msg, ok := c.index[index]
	if !ok {
		return "", nil, false
	}

	return msg.Key, msg.Value, true
}

// Close closes log files.
func (c *Wal) Close() error {
	if err := c.msgs.Close(); err != nil {
		return errors.Wrap(err, "failed to close msgs log file")
	}
	return nil
}
