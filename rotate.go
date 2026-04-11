package gowal

import (
	"maps"

	"github.com/pkg/errors"
)

// rotateIfNeeded rotates the log if needed.
//
// It opens a new segment if the number of records in the current segment exceeds the threshold
// and closes oldest segment if the number of segments exceeds the limit.
func (c *Wal) rotateIfNeeded() error {
	if c.activeSegment.Len() < c.segmentsThreshold {
		return nil
	}

	// remove oldest segment if we're at the segment limit
	if c.nextSegmentNumber >= c.maxSegments {
		if err := c.removeOldestSegment(); err != nil {
			return err
		}
	}

	maps.Copy(c.index, c.activeSegment.index)

	if err := c.activeSegment.Close(); err != nil {
		return errors.Wrap(err, "failed to close log file")
	}

	if err := c.openNewSegment(); err != nil {
		return err
	}

	return nil
}
