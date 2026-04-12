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
	file, err := os.OpenFile(segmentPath, os.O_RDONLY, 0644)
	if err != nil {
		return false, errors.Wrap(err, "failed to open segment file")
	}
	defer file.Close()

	statFd, err := file.Stat()
	if err != nil {
		return false, errors.Wrap(err, "failed to stat segment file")
	}

	if statFd.Size() == 0 {
		if err := os.Remove(segmentPath); err != nil {
			return false, errors.Wrap(err, "failed to remove empty segment")
		}

		return true, nil
	}

	if _, _, err = loadSegmentIndex(file); err == nil {
		return false, nil
	}

	if err := os.Remove(segmentPath); err != nil {
		return false, errors.Wrap(err, "failed to remove corrupted segment")
	}

	return true, nil
}
