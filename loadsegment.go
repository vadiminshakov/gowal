package gowal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal/msg"
	"io"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
)

// segmentInfoAndIndex loads segment info (file descriptor, name, size, etc) and index from segment files for log log.
// Works like loadSegment, but for multiple segments.
func segmentInfoAndIndex(segNumbers []int, path string) (*os.File, os.FileInfo, map[uint64]msg.Msg, error) {
	index := make(map[uint64]msg.Msg)
	var (
		logFileFD      *os.File
		logFileInfo    os.FileInfo
		idxFromSegment map[uint64]msg.Msg
		err            error
	)
	for _, segindex := range segNumbers {
		logFileFD, logFileInfo, idxFromSegment, err = loadSegment(path + strconv.Itoa(segindex))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to load indexes from msg log file")
		}

		maps.Copy(index, idxFromSegment)
	}

	return logFileFD, logFileInfo, index, nil
}

// loadSegment loads segment info (file descriptor, name, size, etc) and index from segment file.
func loadSegment(path string) (fd *os.File, fileinfo os.FileInfo, index map[uint64]msg.Msg, err error) {
	fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to open log segment file")
	}

	index, err = loadIndexes(fd)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to build index from log segment")
	}

	fileinfo, err = fd.Stat()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read log segment file stat")
	}

	return fd, fileinfo, index, nil
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
func loadIndexes(file *os.File) (map[uint64]msg.Msg, error) {
	buf, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read log file")
	}

	if len(buf) == 0 {
		return make(map[uint64]msg.Msg), nil
	}

	var msgs []msg.Msg
	dec := gob.NewDecoder(bytes.NewReader(buf))
	for {
		var msgIndexed msg.Msg
		if err = dec.Decode(&msgIndexed); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to decode indexed msg from log")
		}

		msgs = append(msgs, msgIndexed)
	}

	index := make(map[uint64]msg.Msg, len(msgs))
	for _, idxMsg := range msgs {
		index[idxMsg.Idx] = idxMsg
	}

	return index, nil
}

func extractSegmentNum(segmentName string) (int, error) {
	_, suffix, ok := strings.Cut(segmentName, "_")
	if !ok {
		return 0, fmt.Errorf("failed to cut suffix from log log file name %s", segmentName)
	}
	i, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("failed to convert suffix %s to int", suffix)
	}

	return i, nil
}
