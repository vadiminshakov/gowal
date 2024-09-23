package gowal

import (
	"github.com/vadiminshakov/gowal/msg"
	"log"
	"os"
)

// rotateSegments rotates log segment if threshold is reached.
// If number of items in index file exceeds threshold, start writing to tmp index buffer.
func (c *Wal) rotateSegments(newMsg msg.Msg) {
	multiplier := 1
	if c.segmentsNumber > 1 {
		multiplier = c.segmentsNumber - 1
	}
	// if threshold is reached, start writing to tmp index buffer
	if len(c.index) > segmentThreshold*multiplier {
		// if segments number exceeds the limit, flush tmp index to main index and rm oldest segment
		if c.segmentsNumber > maxSegments {
			c.index = c.tmpIndex
			c.tmpIndex = make(map[uint64]msg.Msg)

			go func(segmentName string) {
				if err := os.Remove(segmentName); err != nil {
					log.Printf("failed to remove oldest segment %s, error: %s", segmentName, err)
				}
			}(c.oldestSegName)
			return
		}
		c.tmpIndex[newMsg.Index()] = newMsg
	}

	return
}
