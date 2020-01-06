package id_generator

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

type idSet struct {
	Ranges   idRanges `json:"ranges"`
	Category string   `json:"category"`
	readOnly bool
}

func newIDSet(ranges []IDRange, category string, readOnly bool) idSet {
	return idSet{
		Ranges:   ranges,
		Category: category,
		readOnly: readOnly,
	}
}

func setFromString(jsn string) (idSet, error) {
	bytes := []byte(jsn)
	set := idSet{}
	err := json.Unmarshal(bytes, &set)
	if err != nil {
		return set, err
	}
	return set, nil
}

func (s *idSet) toString() string {
	bytes, err := json.Marshal(&s)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func (s *idSet) getSize() uint64 {
	var size uint64 = 0
	for _, rng := range s.Ranges {
		size += rng.getSize()
	}
	return size
}

func (s *idSet) takeIDs(setSize uint64) (idSet, error) {
	if s.readOnly {
		return idSet{}, errors.New("ID range is read only")
	}
	if setSize <= 0 {
		return idSet{}, errors.New("ID data size must be greater than 0")
	}
	size := s.getSize()
	if size == 0 {
		return idSet{}, errors.New("can't take ID's, data is empty")
	}
	if setSize > size {
		setSize = size
	}
	var numTaken uint64 = 0
	takenRanges := idRanges{}
	for numTaken < setSize {
		numToBeTaken := setSize - numTaken
		firstRange := s.Ranges[0]
		firstRangeSize := firstRange.getSize()
		var numToBeTakenFromRange = uint64(math.Min(float64(numToBeTaken), float64(firstRangeSize)))
		takenIDRange, err := firstRange.takeIDs(numToBeTakenFromRange)
		if err != nil {
			return idSet{}, err
		}
		takenRanges = append(takenRanges, takenIDRange)
		numTaken += numToBeTakenFromRange

		if !firstRange.hasMoreIDs() {
			s.takeFirstRange()
		} else {
			s.Ranges[0] = firstRange
		}
	}
	return newIDSet(takenRanges, s.Category, false), nil
}

func (s *idSet) takeID() (uint64, error) {
	if s.readOnly {
		return 0, errors.New("ID range is read only")
	}
	size := s.getSize()
	if size == 0 {
		return 0, errors.New("can't take ID's, data is empty")
	}
	firstRange := s.Ranges[0]
	id, err := firstRange.takeID()
	if err != nil {
		return 0, err
	}
	if !firstRange.hasMoreIDs() {
		s.takeFirstRange()
	} else {
		s.Ranges[0] = firstRange
	}
	return id, nil
}

func (s *idSet) peekNextID() (uint64, error) {
	if len(s.Ranges) == 0 {
		return 0, errors.New("no more ID's remaining in data")
	}
	firstRange := s.Ranges[0]
	return firstRange.CurrentStartID, nil
}

func (s *idSet) takeFirstRange() IDRange {
	if len(s.Ranges) == 0 {
		return IDRange{}
	}
	firstRange := IDRange{}
	firstRange, s.Ranges = s.Ranges[0], s.Ranges[1:]
	return firstRange
}

func (s *idSet) hasMoreIDs() bool {
	if len(s.Ranges) == 0 {
		return false
	}
	return s.Ranges[0].hasMoreIDs()
}

func (s *idSet) validateNoOverlap(pushedRange IDRange) error {
	if len(s.Ranges) == 0 {
		return nil
	}
	previousRange := s.Ranges.lower(pushedRange)
	if previousRange != nil {
		if previousRange.EndID > pushedRange.CurrentStartID {
			return errors.New(fmt.Sprintf("pushed range %v overlaps with range %v in idSet %v", pushedRange, previousRange, s))
		}
	} else {
		firstRange := s.Ranges[0]
		if pushedRange.EndID >= firstRange.CurrentStartID {
			return errors.New(fmt.Sprintf("pushed range %v overlaps with range %v in idSet %v", pushedRange, firstRange, s))
		}
	}
	return nil
}

func (s *idSet) addIDRange(newRange IDRange) {
	var merged bool
	const maxAttempts = 1
	var attempts = 0
	for !merged && attempts <= maxAttempts {
		previousRange := s.Ranges.lower(newRange)
		if previousRange != nil {
			combinedNewRange := previousRange.tryMerge(newRange)
			if combinedNewRange != nil {
				merged = true
				newRange = *combinedNewRange
				s.removeRange(*previousRange)
			}
		}
		nextRange := s.Ranges.higher(newRange)
		if nextRange != nil {
			combinedNewRange := newRange.tryMerge(*nextRange)
			if combinedNewRange != nil {
				merged = true
				newRange = *combinedNewRange
				s.removeRange(*nextRange)
			}
		}
		attempts++
	}
	s.addRange(newRange)
}

func (s *idSet) pushIDs(pushedIDSet idSet) error {
	if pushedIDSet.Category != s.Category {
		return errors.New(fmt.Sprintf("can't push ID data %v to ID data %v, categories don't match\n", pushedIDSet, s))
	}
	if pushedIDSet.readOnly {
		return errors.New(fmt.Sprintf("data %v is readOnly\n", pushedIDSet))
	}
	if s.readOnly {
		return errors.New(fmt.Sprintf("data %v is readOnly\n", s))
	}
	for _, r := range pushedIDSet.Ranges {
		if err := s.validateNoOverlap(r); err != nil {
			return err
		}
	}
	for pushedIDSet.hasMoreIDs() {
		pushedIDRange := pushedIDSet.takeFirstRange()
		s.addIDRange(pushedIDRange)
	}
	return nil
}

func (s *idSet) addRange(rng IDRange) {
	s.Ranges = append(s.Ranges, rng)
}

func (s *idSet) removeRange(rmv IDRange) {
	for i, rng := range s.Ranges {
		if rng.CurrentStartID == rmv.CurrentStartID &&
			rng.EndID == rmv.EndID {
			s.Ranges = append(s.Ranges[:i], s.Ranges[i+1:]...)
		}
	}
}
