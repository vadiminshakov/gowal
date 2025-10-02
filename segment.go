package gowal

import (
	"fmt"
	"github.com/pkg/errors"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"io"
	"maps"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

// removeOldestSegment deletes the oldest segment.
func (c *Wal) removeOldestSegment() error {
	oldestSegment := c.oldestSegmentName()

	// load index of the segment we're about to remove to clean up main index
	oldestSegmentIndex, err := loadIndexFromSegment(oldestSegment)
	if err != nil {
		return errors.Wrap(err, "failed to load index of oldest segment")
	}

	// remove the segment file
	if err := os.Remove(oldestSegment); err != nil {
		return errors.Wrap(err, "failed to remove oldest segment")
	}

	// remove entries from main index that belonged to the deleted segment
	for idx := range oldestSegmentIndex {
		delete(c.index, idx)
	}

	return nil
}

// openNewSegment creates new segment.
func (c *Wal) openNewSegment() error {
	newSegmentName := path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(c.segmentsNumber))
	logFile, err := os.OpenFile(newSegmentName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}

	c.segmentsNumber++

	// transfer tmpIndex to main index when creating new segment
	maps.Copy(c.index, c.tmpIndex)
	c.tmpIndex = make(map[uint64]msg)

	c.log = logFile
	c.lastOffset = 0

	return nil
}

// oldestSegmentName returns name of the oldest segment.
func (c *Wal) oldestSegmentName() string {
	oldestSegmentNumber := c.segmentsNumber - c.maxSegments
	return path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(oldestSegmentNumber))
}

// segmentInfoAndIndex loads segment info (file descriptor, name, size, etc) and index from segment files.
// Works like loadSegment, but for multiple segments.
func segmentInfoAndIndex(segNumbers []int, path string) (*os.File, int64, map[uint64]msg, error) {
	index := make(map[uint64]msg)
	var (
		logFileFD      *os.File
		lastOffset     int64
		idxFromSegment map[uint64]msg
		err            error
	)
	for _, segindex := range segNumbers {
		if logFileFD != nil {
			logFileFD.Close()
		}

		logFileFD, lastOffset, idxFromSegment, err = loadSegment(path + strconv.Itoa(segindex))
		if err != nil {
			return nil, 0, nil, errors.Wrap(err, "failed to load indexes from msg log file")
		}

		maps.Copy(index, idxFromSegment)
	}

	return logFileFD, lastOffset, index, nil
}

// removeCorruptedSegments removes corrupted segments.
func removeCorruptedSegments(segmentNumbers []int, basePath string) ([]string, error) {
	var removedFiles []string

	for _, segmentNumber := range segmentNumbers {
		segmentPath := basePath + strconv.Itoa(segmentNumber)
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

// loadSegment loads segment info (file descriptor, name, size, etc) and index from segment file.
func loadSegment(path string) (fd *os.File, lastOffset int64, index map[uint64]msg, err error) {
	fd, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, 0, nil, errors.Wrap(err, "failed to open log segment file")
	}

	lastOffset, err = calculateLastOffset(fd)
	if err != nil {
		return nil, 0, nil, errors.Wrap(err, "failed to calculate last offset")
	}

	index, err = loadIndexes(fd)
	if err != nil {
		return nil, 0, nil, errors.Wrap(err, "failed to build index from log segment")
	}

	return fd, lastOffset, index, nil
}

func calculateLastOffset(fd *os.File) (int64, error) {
	fileInfo, err := fd.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %v", err)
	}

	if fileInfo.Size() == 0 {
		return 0, nil
	}

	return fileInfo.Size() + 1, nil
}

// handleCorruptedSegment checks segment for corruption and removes if corrupted.
func handleCorruptedSegment(segmentPath string) (bool, error) {
	file, err := os.OpenFile(segmentPath, os.O_RDONLY, 0644)
	if err != nil {
		return false, errors.Wrap(err, "failed to open segment file")
	}
	defer file.Close()

	statFd, err := file.Stat()
	if err != nil {
		return false, err
	}

	if statFd.Size() == 0 {
		return true, nil
	}

	// try to load indexes - this will verify checksums
	_, err = loadIndexes(file)
	if err == nil {
		return false, nil // No corruption detected
	}

	// Corruption detected, remove segment
	if err := os.Remove(segmentPath); err != nil {
		return false, errors.Wrap(err, "failed to remove corrupted segment")
	}

	return true, nil
}

// findSegmentNumbers finds all segment numbers in the directory.
func findSegmentNumber(dir string, prefix string) (segmentsNumbers []int, err error) {
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return nil, errors.Wrap(err, "failed to create dir for wal")
		}
	}
	de, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read dir for wal")
	}

	segmentsNumbers = make([]int, 0)
	for _, d := range de {
		if d.IsDir() {
			continue
		}

		if strings.HasPrefix(d.Name(), prefix) {
			i, err := extractSegmentNum(d.Name())
			if err != nil {
				return nil, errors.Wrap(err, "initialization failed: failed to extract segment number from wal file name")
			}

			segmentsNumbers = append(segmentsNumbers, i)
		}
	}

	sort.Slice(segmentsNumbers, func(i, j int) bool {
		return segmentsNumbers[i] < segmentsNumbers[j]
	})

	if len(segmentsNumbers) == 0 {
		segmentsNumbers = append(segmentsNumbers, 0)
	}

	return segmentsNumbers, nil
}

// loadIndexes loads index from log file.
func loadIndexes(file *os.File) (map[uint64]msg, error) {
	file.Seek(0, io.SeekStart)

	index := make(map[uint64]msg)
	dec := msgpack.NewDecoder(file)

	for {
		var msgIndexed msg
		if err := dec.Decode(&msgIndexed); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to decode indexed msg from log")
		}

		// verify checksum for each loaded message
		if err := msgIndexed.verifyChecksum(); err != nil {
			return nil, errors.Wrapf(err, "corrupted message at index %d", msgIndexed.Idx)
		}

		index[msgIndexed.Idx] = msgIndexed
	}

	return index, nil
}

func extractSegmentNum(segmentName string) (int, error) {
	_, suffix, ok := strings.Cut(segmentName, "_")
	if !ok {
		return 0, fmt.Errorf("failed to cut suffix from log file name %s", segmentName)
	}
	i, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("failed to convert suffix %s to int", suffix)
	}

	return i, nil
}

// loadIndexFromSegment loads only the index from a segment file
func loadIndexFromSegment(segmentPath string) (map[uint64]msg, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open segment file for index loading")
	}
	defer file.Close()

	return loadIndexes(file)
}
