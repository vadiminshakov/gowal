package gowal

import (
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal/msg"
)

// rotateIfNeeded rotates the log if needed.
//
// It opens a new segment if the number of records in the log exceeds the threshold
// and closes oldest segment if the number of segments exceeds the limit.
func (c *Wal) rotateIfNeeded(index uint64, key string, value []byte) error {
	if len(c.index) < c.segmentsThreshold {
		return nil
	}

	if len(c.index) >= c.segmentsNumber*c.segmentsThreshold {
		// remove oldest segment if the number of segments exceeds the limit
		if c.segmentsNumber >= c.maxSegments {
			if err := c.removeOldestSegment(); err != nil {
				return err
			}
		}

		// close current segment and open new one
		if err := c.log.Close(); err != nil {
			return errors.Wrap(err, "failed to close log file")
		}

		if err := c.openNewSegment(); err != nil {
			return err
		}
	}

	c.tmpIndex[index] = msg.Msg{Key: key, Value: value, Idx: index}

	return nil
}
