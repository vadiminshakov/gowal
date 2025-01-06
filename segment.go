package gowal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
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
	if err := os.Remove(oldestSegment); err != nil {
		return errors.Wrap(err, "failed to remove oldest segment")
	}

	if err := os.Remove(oldestSegment + checkSumPostfix); err != nil {
		return errors.Wrap(err, "failed to remove oldest segment checksum file")
	}

	c.index = c.tmpIndex
	c.tmpIndex = make(map[uint64]msg)

	return nil
}

// openNewSegment creates new segment.
func (c *Wal) openNewSegment() error {
	newSegmentName := path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(c.segmentsNumber))
	logFile, err := os.OpenFile(newSegmentName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}

	checksumFile, err := os.OpenFile(newSegmentName+checkSumPostfix, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}

	c.segmentsNumber++

	c.log = logFile
	c.checksum = checksumFile
	c.lastOffset = 0

	c.buf.Reset()
	c.enc = gob.NewEncoder(c.buf)

	return nil
}

// oldestSegmentName returns name of the oldest segment.
func (c *Wal) oldestSegmentName() string {
	oldestSegmentNumber := c.segmentsNumber - c.maxSegments
	if oldestSegmentNumber < 0 {
		oldestSegmentNumber = 0
	}
	return path.Join(c.pathToLogsDir, c.prefix+strconv.Itoa(oldestSegmentNumber))
}

// segmentInfoAndIndex loads segment info (file descriptor, name, size, etc) and index from segment files.
// Works like loadSegment, but for multiple segments.
func segmentInfoAndIndex(segNumbers []int, path string) (*os.File, *os.File, int64, map[uint64]msg, error) {
	index := make(map[uint64]msg)
	var (
		logFileFD      *os.File
		checksumFd     *os.File
		lastOffset     int64
		idxFromSegment map[uint64]msg
		err            error
	)
	for _, segindex := range segNumbers {
		logFileFD, checksumFd, lastOffset, idxFromSegment, err = loadSegment(path + strconv.Itoa(segindex))
		if err != nil {
			return nil, nil, 0, nil, errors.Wrap(err, "failed to load indexes from msg log file")
		}

		maps.Copy(index, idxFromSegment)
	}

	return logFileFD, checksumFd, lastOffset, index, nil
}

// removeCorruptedSegments removes corrupted segments and their checksums.
func removeCorruptedSegments(segmentNumbers []int, basePath string) ([]string, error) {
	var removedFiles []string

	for _, segmentNumber := range segmentNumbers {
		segmentPath := basePath + strconv.Itoa(segmentNumber)
		removed, err := handleCorruptedSegment(segmentPath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to process segment %s", segmentPath)
		}
		if removed {
			removedFiles = append(removedFiles, segmentPath, segmentPath+checkSumPostfix)
		}
	}

	return removedFiles, nil
}

// loadSegment loads segment info (file descriptor, name, size, etc) and index from segment file.
func loadSegment(path string) (fd *os.File, checksumFd *os.File, lastOffset int64, index map[uint64]msg, err error) {
	fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, nil, 0, nil, errors.Wrap(err, "failed to open log segment file")
	}

	chk, err := os.OpenFile(path+checkSumPostfix, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, nil, 0, nil, errors.Wrap(err, "failed to cheksum file")
	}

	if err = compareChecksums(fd, chk); err != nil {
		return nil, nil, 0, nil, errors.Wrap(err, "failed to compare checksums")
	}

	lastOffset, err = calculateLastOffset(fd)
	if err != nil {
		return nil, nil, 0, nil, errors.Wrap(err, "failed to calculate last offset")
	}

	index, err = loadIndexes(fd)
	if err != nil {
		return nil, nil, 0, nil, errors.Wrap(err, "failed to build index from log segment")
	}

	return fd, chk, lastOffset, index, nil
}

func calculateLastOffset(fd *os.File) (int64, error) {
	var lastOffset int64

	decoder := gob.NewDecoder(fd)
	for {
		var msg msg
		offset, err := fd.Seek(io.SeekCurrent, 0)
		if err != nil {
			return 0, errors.Wrap(err, "failed to get current offset")
		}

		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}

			// set offset to the beginning of the file
			if _, err := fd.Seek(0, io.SeekStart); err != nil {
				return 0, errors.Wrap(err, "failed to seek to the beginning of the file")
			}

			return lastOffset, nil
		}

		lastOffset = offset
	}

	// set offset to the beginning of the file
	if _, err := fd.Seek(lastOffset, io.SeekStart); err != nil {
		return 0, errors.Wrap(err, "failed to seek to the beginning of the file")
	}

	return lastOffset, nil
}

// handleCorruptedSegment checks the checksum and removes the segment and checksum files if corrupted.
func handleCorruptedSegment(segmentPath string) (bool, error) {
	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return false, errors.Wrap(err, "failed to open segment file")
	}
	defer file.Close()

	checksumFile, err := os.OpenFile(segmentPath+checkSumPostfix, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return false, errors.Wrap(err, "failed to open checksum file")
	}
	defer checksumFile.Close()

	if err := compareChecksums(file, checksumFile); err == nil {
		return false, nil // Checksums match; no need to erase.
	}

	if err := os.Remove(segmentPath); err != nil {
		return false, errors.Wrap(err, "failed to remove corrupted segment")
	}

	if err := os.Remove(segmentPath + checkSumPostfix); err != nil {
		return false, errors.Wrap(err, "failed to remove corrupted checksum file")
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

		if strings.HasSuffix(d.Name(), checkSumPostfix) {
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
	buf, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read log file")
	}

	if len(buf) == 0 {
		return make(map[uint64]msg), nil
	}

	var msgs []msg
	dec := gob.NewDecoder(bytes.NewReader(buf))

	for {
		var msgIndexed msg
		if err = dec.Decode(&msgIndexed); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to decode indexed msg from log")
		}

		msgs = append(msgs, msgIndexed)
	}

	index := make(map[uint64]msg, len(msgs))
	for _, idxMsg := range msgs {
		index[idxMsg.Idx] = idxMsg
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
