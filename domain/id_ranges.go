package domain

import (
	"sort"
)

type idRanges []IDRange

func (r idRanges) lower(pushedRange IDRange) *IDRange {
	r.sort()
	var previousRange *IDRange
	for i := len(r) - 1; i >= 0; i-- {
		if r[i].CurrentStartID < pushedRange.CurrentStartID &&
			r[i].EndID < pushedRange.EndID {
			previousRange = &r[i]
			break
		}
	}
	return previousRange
}

func (r idRanges) higher(pushedRange IDRange) *IDRange {
	r.sort()
	var nextRange *IDRange
	for i := 0; i < len(r); i++ {
		if r[i].CurrentStartID > pushedRange.CurrentStartID &&
			r[i].EndID > pushedRange.EndID {
			nextRange = &r[i]
			break
		}
	}
	return nextRange
}

func (r idRanges) sort() {
	sort.Slice(r, func(i, j int) bool {
		return r[i].CurrentStartID < r[j].CurrentStartID &&
			r[i].EndID < r[j].EndID
	})
}
