package id_generator

import (
	"errors"
)

type IDRange struct {
	CurrentStartID uint64 `json:"currentStartID"`
	EndID          uint64 `json:"endID"`
	readOnly       bool
}

func NewIDRange(currStartID, endID uint64, readOnly bool) IDRange {
	return IDRange{
		CurrentStartID: currStartID,
		EndID:          endID,
		readOnly:       readOnly,
	}
}

func (i *IDRange) getSize() (size uint64) {
	if i.EndID > 0 && i.CurrentStartID > 0 {

		size = i.EndID - i.CurrentStartID + 1
	}
	return
}

func (i *IDRange) getStartID() uint64 {
	return i.CurrentStartID
}

func (i *IDRange) getEndID() uint64 {
	return i.EndID
}

func (i *IDRange) takeIDs(idRangeSize uint64) (IDRange, error) {
	if i.readOnly {
		return IDRange{}, errors.New("ID range is read only")
	}
	size := i.getSize()
	if size == 0 {
		return IDRange{}, errors.New("can't take ID's, range is empty")
	}
	if idRangeSize > size {
		idRangeSize = size
	}
	takenStartID := i.CurrentStartID
	i.CurrentStartID = takenStartID + idRangeSize
	newEnd := takenStartID + idRangeSize - 1
	return NewIDRange(takenStartID, newEnd, false), nil
}

func (i *IDRange) takeID() (uint64, error) {
	if i.readOnly {
		return 0, errors.New("ID range is read only")
	}
	currAt := i.CurrentStartID
	i.CurrentStartID++
	return currAt, nil
}

func (i *IDRange) hasMoreIDs() bool {
	return i.CurrentStartID <= i.EndID
}

func (i *IDRange) tryMerge(newRange IDRange) *IDRange {
	if !i.isAdjacent(newRange) {
		return nil
	}
	r := NewIDRange(i.CurrentStartID, newRange.EndID, false)
	return &r
}

func (i *IDRange) isAdjacent(newRange IDRange) bool {
	return i.EndID == newRange.CurrentStartID-1
}

func (i *IDRange) setReadOnly(r bool) {
	i.readOnly = r
}

func (i *IDRange) copy(r bool) IDRange {
	return NewIDRange(i.CurrentStartID, i.EndID, r)
}
