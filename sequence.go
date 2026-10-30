package gowal

import "github.com/pkg/errors"

// ErrIndexGap is returned when the log on disk is not a contiguous sequence of indexes.
var ErrIndexGap = errors.New("gap in log indexes")

// indexSequence checks that records read from the log form a contiguous sequence.
type indexSequence struct {
	last    uint64
	started bool
}

func (s *indexSequence) next(record Record) error {
	if record.Index == 0 {
		return errors.Wrap(ErrIndexGap, "record index must be greater than 0")
	}

	if record.Tombstone {
		if s.started && record.Index > s.last {
			return errors.Wrapf(ErrIndexGap, "tombstone index %d exceeds last index %d", record.Index, s.last)
		}
		if !s.started && record.Index > s.last {
			s.last = record.Index
		}
		return nil
	}

	if !s.started {
		if record.Index <= s.last {
			return errors.Wrapf(ErrIndexGap, "expected index after %d, got %d", s.last, record.Index)
		}
		s.last = record.Index
		s.started = true
		return nil
	}

	if record.Index != s.last+1 {
		return errors.Wrapf(ErrIndexGap, "expected index %d, got %d", s.last+1, record.Index)
	}

	s.last = record.Index
	return nil
}
