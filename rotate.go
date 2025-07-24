package gowal

import (
	"github.com/pkg/errors"
)

// rotateIfNeeded rotates the log if needed.
//
// It opens a new segment if the number of records in the current segment exceeds the threshold
// and closes oldest segment if the number of segments exceeds the limit.
func (c *Wal) rotateIfNeeded(index uint64, key string, value []byte) error {
	// Add to temporary index first
	c.tmpIndex[index] = msg{Key: key, Value: value, Idx: index}

	// Check if current segment needs rotation based on tmpIndex size
	if len(c.tmpIndex) < c.segmentsThreshold {
		return nil
	}

	// кemove oldest segment if we're at the segment limit
	if c.segmentsNumber >= c.maxSegments {
		if err := c.removeOldestSegment(); err != nil {
			return err
		}
	}

	// сlose current segment and open new one
	if err := c.log.Close(); err != nil {
		return errors.Wrap(err, "failed to close log file")
	}

	if err := c.openNewSegment(); err != nil {
		return err
	}

	return nil
}
