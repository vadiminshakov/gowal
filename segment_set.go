package gowal

import (
	"maps"
	"os"

	"github.com/pkg/errors"
)

type segmentSet struct {
	namer                    segmentNamer
	active                   *segment
	activeNumber             segmentNumber
	historicalIndex          map[uint64]Record
	historicalIndexBySegment map[segmentNumber]map[uint64]Record
	numbers                  []segmentNumber
	next                     segmentNumber
	threshold                int
	max                      int
}

func openSegmentSet(config Config) (*segmentSet, uint64, error) {
	namer := segmentNamer{dir: config.Dir, prefix: config.Prefix}

	numbers, err := findSegmentNumbers(config.Dir, namer)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to find segment numbers")
	}

	active, historical, historicalBySegment, lastIndex, err := openSegments(numbers, namer)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to load log segments")
	}

	return &segmentSet{
		namer:                    namer,
		active:                   active,
		activeNumber:             numbers[len(numbers)-1],
		historicalIndex:          historical,
		historicalIndexBySegment: historicalBySegment,
		numbers:                  numbers,
		next:                     nextSegmentNumber(numbers),
		threshold:                config.SegmentThreshold,
		max:                      config.MaxSegments,
	}, lastIndex, nil
}

func openSegments(numbers []segmentNumber, namer segmentNamer) (*segment, map[uint64]Record, map[segmentNumber]map[uint64]Record, uint64, error) {
	historical := make(map[uint64]Record)
	historicalBySegment := make(map[segmentNumber]map[uint64]Record)

	var lastIndex uint64
	var active *segment

	for i, number := range numbers {
		s, err := openSegment(namer.path(number))
		if err != nil {
			return nil, nil, nil, 0, errors.Wrap(err, "failed to load indexes from record log file")
		}
		if s.LastIndex() > lastIndex {
			lastIndex = s.LastIndex()
		}

		if i < len(numbers)-1 {
			maps.Copy(historical, s.index)
			historicalBySegment[number] = s.index

			if err := s.Close(); err != nil {
				return nil, nil, nil, 0, errors.Wrap(err, "failed to close historical segment")
			}

			continue
		}

		active = s
	}

	return active, historical, historicalBySegment, lastIndex, nil
}

func (s *segmentSet) append(records []Record) error {
	if err := s.rotateIfNeeded(); err != nil {
		return err
	}

	if err := s.active.Append(records); err != nil {
		return err
	}

	return nil
}

func (s *segmentSet) syncActive() error {
	return s.active.Sync()
}

func (s *segmentSet) close() error {
	return s.active.Close()
}

func (s *segmentSet) record(index uint64) (Record, bool) {
	if record, ok := s.active.Record(index); ok {
		return record, true
	}
	if record, ok := s.historicalIndex[index]; ok {
		return record.clone(), true
	}
	return Record{}, false
}

func (s *segmentSet) records() []Record {
	all := make(map[uint64]Record, len(s.historicalIndex)+s.active.Len())
	maps.Copy(all, s.historicalIndex)
	maps.Copy(all, s.active.index)

	records := make([]Record, 0, len(all))
	for _, record := range all {
		records = append(records, record.clone())
	}

	return records
}

func (s *segmentSet) forgetHistorical(index uint64) {
	if _, ok := s.historicalIndex[index]; !ok {
		return
	}

	delete(s.historicalIndex, index)
	for number, segmentIndex := range s.historicalIndexBySegment {
		delete(segmentIndex, index)
		if len(segmentIndex) == 0 {
			delete(s.historicalIndexBySegment, number)
		}
	}
}

func (s *segmentSet) rotateIfNeeded() error {
	if s.active.Len() < s.threshold {
		return nil
	}

	if err := s.active.Close(); err != nil {
		return errors.Wrap(err, "failed to close log file")
	}

	activeIndex := s.active.index
	maps.Copy(s.historicalIndex, activeIndex)
	s.historicalIndexBySegment[s.activeNumber] = activeIndex

	if s.max > 0 && len(s.numbers) >= s.max {
		if err := s.removeOldestSegment(); err != nil {
			return err
		}
	}

	active, err := openSegment(s.namer.path(s.next))
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}

	s.active = active
	s.activeNumber = s.next
	s.numbers = append(s.numbers, s.next)
	s.next++

	return nil
}

func (s *segmentSet) removeOldestSegment() error {
	if len(s.numbers) == 0 {
		return nil
	}

	oldest := s.numbers[0]
	if err := s.removeSegment(oldest); err != nil {
		return err
	}

	s.numbers = s.numbers[1:]
	return nil
}

func (s *segmentSet) removeSegment(number segmentNumber) error {
	segmentIndex := s.historicalIndexBySegment[number]

	if err := removeSegmentFile(s.namer.path(number)); err != nil {
		return err
	}

	for idx, removed := range segmentIndex {
		current, ok := s.historicalIndex[idx]
		if ok && current.sameRecord(removed) {
			delete(s.historicalIndex, idx)
		}
	}
	delete(s.historicalIndexBySegment, number)

	return nil
}

func removeSegmentFile(segmentPath string) error {
	if err := os.Remove(segmentPath); err != nil {
		return errors.Wrap(err, "failed to remove oldest segment")
	}
	return nil
}
