package gowal

import (
	"encoding/gob"
	"github.com/pkg/errors"
	"os"
	"path"
	"strconv"
)

// removeOldestSegment deletes the oldest segment.
func (c *Wal) removeOldestSegment() error {
	oldestSegment := c.oldestSegmentName()
	if err := os.Remove(oldestSegment); err != nil {
		return errors.Wrap(err, "failed to remove oldest segment")
	}

	if err := os.Remove(oldestSegment + checkSumPostfix); err != nil {
		return errors.Wrap(err, "failed to remove oldest segment checksum file")
	}

	c.index = c.tmpIndex
	c.tmpIndex = make(map[uint64]msg)

	return nil
}

// openNewSegment creates new segment.
func (c *Wal) openNewSegment() error {
	newSegmentName := path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(c.segmentsNumber))
	logFile, err := os.OpenFile(newSegmentName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}

	checksumFile, err := os.OpenFile(newSegmentName+checkSumPostfix, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}

	c.segmentsNumber++

	c.log = logFile
	c.checksum = checksumFile
	c.lastOffset = 0

	c.buf.Reset()
	c.enc = gob.NewEncoder(c.buf)

	return nil
}

// oldestSegmentName returns name of the oldest segment.
func (c *Wal) oldestSegmentName() string {
	oldestSegmentNumber := c.segmentsNumber - c.maxSegments
	if oldestSegmentNumber < 0 {
		oldestSegmentNumber = 0
	}
	return path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(oldestSegmentNumber))
}

// Get queries value at specific index in the log log.
func (c *Wal) Get(index uint64) (string, []byte, bool) {
	msg, ok := c.index[index]
	if !ok {
		return "", nil, false
	}

	return msg.Key, msg.Value, true
}
