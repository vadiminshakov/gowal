package gowal

import (
	"os"

	"github.com/pkg/errors"
)

// RecoverResult describes what UnsafeRecover changed on disk.
type RecoverResult struct {
	// Truncated is the segment cut at the first broken frame or index gap, or "" if no segment was cut.
	// Only one segment can be cut: all segments after it are removed.
	Truncated string

	// Removed are the deleted segments: empty ones, the broken one if it had no valid frames,
	// and all segments after the broken one.
	Removed []string

	// LastIndex is the last index of the remaining log.
	LastIndex uint64
}

// cutLog keeps the longest valid contiguous prefix of the log
// and drops everything after the first broken frame or index gap.
func cutLog(numbers []segmentNumber, namer segmentNamer) (RecoverResult, error) {
	var result RecoverResult
	var seq indexSequence

	remove := func(segmentPath string) error {
		if err := removeSegmentFile(segmentPath); err != nil {
			return err
		}
		result.Removed = append(result.Removed, segmentPath)
		return nil
	}

	for i, number := range numbers {
		segmentPath := namer.path(number)

		validSize, broken, err := scanSegment(segmentPath, &seq)
		if err != nil {
			return result, errors.Wrapf(err, "failed to scan segment %s", segmentPath)
		}

		switch {
		case validSize == 0:
			if err := remove(segmentPath); err != nil {
				return result, err
			}
		case broken:
			if err := truncateSegment(segmentPath, validSize); err != nil {
				return result, errors.Wrapf(err, "failed to truncate segment %s", segmentPath)
			}
			result.Truncated = segmentPath
		}

		if !broken {
			continue
		}

		for _, next := range numbers[i+1:] {
			if err := remove(namer.path(next)); err != nil {
				return result, err
			}
		}

		break
	}

	result.LastIndex = seq.last

	return result, nil
}

// scanSegment reads the segment and finds where its valid frames end.
// It reports the size of the valid prefix and whether a broken frame or index gap follows it.
func scanSegment(segmentPath string, seq *indexSequence) (int64, bool, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		return 0, false, errors.Wrap(err, "failed to open segment file")
	}
	defer file.Close()

	validSize, scanErr := scanFrames(file, seq, func(Record) {})
	if scanErr == nil {
		return validSize, false, nil
	}

	// a failed read is not a broken frame, so the data must not be dropped
	var pathErr *os.PathError
	if errors.As(scanErr, &pathErr) {
		return 0, false, errors.Wrap(scanErr, "failed to read segment file")
	}

	return validSize, true, nil
}

func truncateSegment(segmentPath string, size int64) error {
	file, err := os.OpenFile(segmentPath, os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "failed to open segment file")
	}
	defer file.Close()

	if err := file.Truncate(size); err != nil {
		return errors.Wrap(err, "failed to truncate segment file")
	}
	if err := file.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync truncated segment file")
	}

	return nil
}
