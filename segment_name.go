package gowal

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type segmentNumber int

type segmentNamer struct {
	dir    string
	prefix string
}

func (n segmentNamer) path(number segmentNumber) string {
	return path.Join(n.dir, n.prefix+strconv.Itoa(int(number)))
}

func (n segmentNamer) parse(fileName string) (segmentNumber, bool, error) {
	suffix, ok := strings.CutPrefix(fileName, n.prefix)
	if !ok {
		return 0, false, nil
	}

	number, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, true, fmt.Errorf("failed to convert suffix %s to int", suffix)
	}

	return segmentNumber(number), true, nil
}

func findSegmentNumbers(dir string, namer segmentNamer) ([]segmentNumber, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create dir for wal")
	}

	de, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read dir for wal")
	}

	segmentNumbers := make([]segmentNumber, 0)
	for _, d := range de {
		if d.IsDir() {
			continue
		}

		number, ok, err := namer.parse(d.Name())
		if err != nil {
			return nil, errors.Wrap(err, "initialization failed: failed to extract segment number from wal file name")
		}
		if ok {
			segmentNumbers = append(segmentNumbers, number)
		}
	}

	sort.Slice(segmentNumbers, func(i, j int) bool {
		return segmentNumbers[i] < segmentNumbers[j]
	})

	if len(segmentNumbers) == 0 {
		segmentNumbers = append(segmentNumbers, 0)
	}

	return segmentNumbers, nil
}

func nextSegmentNumber(segmentNumbers []segmentNumber) segmentNumber {
	return segmentNumbers[len(segmentNumbers)-1] + 1
}

func segmentPath(dir, prefix string, number int) string {
	return segmentNamer{dir: dir, prefix: prefix}.path(segmentNumber(number))
}
