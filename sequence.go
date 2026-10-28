package gowal

import "github.com/pkg/errors"

// ErrIndexGap is returned when the log on disk is not a contiguous sequence of indexes.
var ErrIndexGap = errors.New("gap in log indexes")

// indexSequence checks that records read from the log form a contiguous sequence.
// A record with an index not greater than the last one is a tombstone and is allowed.
// The first record of the log may have any index, because the oldest segments may have been removed.
type indexSequence struct {
	last    uint64
	started bool
}

func (s *indexSequence) next(index uint64) error {
	if s.started && index > s.last+1 {
		return errors.Wrapf(ErrIndexGap, "expected index %d, got %d", s.last+1, index)
	}

	if !s.started || index > s.last {
		s.last = index
	}
	s.started = true

	return nil
}
