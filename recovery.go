package gowal

import (
	"os"

	"github.com/pkg/errors"
)

func removeCorruptedSegments(numbers []segmentNumber, namer segmentNamer) ([]string, error) {
	var removedFiles []string

	for _, number := range numbers {
		segmentPath := namer.path(number)
		removed, err := handleCorruptedSegment(segmentPath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to process segment %s", segmentPath)
		}
		if removed {
			removedFiles = append(removedFiles, segmentPath)
		}
	}

	return removedFiles, nil
}

func handleCorruptedSegment(segmentPath string) (bool, error) {
	info, err := os.Stat(segmentPath)
	if err != nil {
		return false, errors.Wrap(err, "failed to stat segment file")
	}

	if info.Size() == 0 {
		if err := os.Remove(segmentPath); err != nil {
			return false, errors.Wrap(err, "failed to remove empty segment")
		}

		return true, nil
	}

	seg, err := openSegment(segmentPath)
	if err == nil {
		_ = seg.Close()
		return false, nil
	}

	if err := os.Remove(segmentPath); err != nil {
		return false, errors.Wrap(err, "failed to remove corrupted segment")
	}

	return true, nil
}
