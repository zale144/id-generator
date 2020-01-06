package id_generator

import (
	"errors"
	"fmt"
	"log"
)

const (
	maxTryCount             = 100
	DefaultIDSetSize        = 10
	defaultTotalSize uint64 = 18446744073709551615
)

type IDGenerator struct {
	idProvider IDProvider
	idSets     map[string]*IDSet
	idReqChan  chan idReq
}

type IDProvider interface {
	GetData(category string) (string, int32, error)
	SetData(data, category string, version int32) error
	Initialize(iniSet string, category string) error
	Delete(category string, version int32) error
	Lock(category string) (interface{}, error)
	Unlock(lck interface{}) error
}

func NewIDGenerator(provider IDProvider) *IDGenerator {

	gen := &IDGenerator{
		idProvider: provider,
		idSets:     make(map[string]*IDSet),
		idReqChan:  make(chan idReq),
	}
	go gen.takeIDHandler()

	return gen
}

func (g *IDGenerator) Initialize(category string, startID uint64) error {
	set, err := g.PeekIDs(category)
	if err != nil {
		log.Println(err.Error())
	}
	if set != nil && set.GetSize() != 0 {
		log.Println("set for category already exists")
		return nil
	}
	currIDs := NewIDSet([]IDRange{
		NewIDRange(startID, defaultTotalSize, false),
	}, category, false)
	return g.idProvider.Initialize(currIDs.String(), category)
}

func (g *IDGenerator) TakeIDsWithRetry(category string) (s *IDSet, rErr error) {
	currTryCount := 0
	success := false
	var takenIDs *IDSet
	var errFin error

	lock, err := g.idProvider.Lock(category)
	if err != nil {
		return nil, err
	}
	defer g.idProvider.Unlock(lock)

	for !success && currTryCount <= maxTryCount {

		if currTryCount > 1 {
			log.Println(fmt.Sprintf("attempt %d of %d", currTryCount, maxTryCount))
		}
		currTryCount++

		// get data
		currData, version, err := g.idProvider.GetData(category)
		if err != nil {
			return nil, err
		}
		// TODO - probably delete
		if len(currData) == 0 {
			if err = g.Initialize(category, 1); err != nil {
				return nil, err
			}
			continue
		}
		// deserialize data
		currIDs, err := IDSetFromString(currData)
		if err != nil {
			return nil, err
		}
		// take IDs
		takenIDs, err = currIDs.TakeIDs(DefaultIDSetSize)
		if err != nil {
			return nil, err
		}

		// try to set data
		setStr := takenIDs.String()
		size := takenIDs.GetSize()
		if errFin = g.idProvider.SetData(currIDs.String(), category, version); errFin != nil {
			log.Println("error saving data", errFin)
		} else {
			success = true
			log.Println("fetched a new ID batch from provider = ", "SET: ", setStr, "SIZE: ", size)
		}
	}
	if !success {
		errMsg := "failed to take IDs"
		log.Println(errMsg, errFin)
		return nil, errors.New(fmt.Sprintf(errMsg+": %s", errFin))
	}
	g.idSets[category] = takenIDs
	return takenIDs, nil
}

func (g *IDGenerator) PushIDsWithRetry(category string) (v int32, rErr error) {
	idSet, ok := g.idSets[category]
	if !ok {
		return -1, errors.New(fmt.Sprintf("no set for category '%s'\n", category))
	}
	if idSet.IsReadOnly() {
		return -1, errors.New("cannot push IDs, ID set is read only")
	}

	currTryCount := 0
	success := false
	var errFin error
	var version int32

	lock, err := g.idProvider.Lock(category)
	if err != nil {
		return -1, err
	}
	defer g.idProvider.Unlock(lock)

	for !success && currTryCount <= maxTryCount {
		if currTryCount > 1 {
			log.Println(fmt.Sprintf("attempt %d of %d", currTryCount, maxTryCount))
		}
		currTryCount++

		// get data
		currData, ver, err := g.idProvider.GetData(category)
		if err != nil {
			return -1, err
		}
		if idSet.GetSize() == 0 {
			return ver, errors.New("cannot push IDs, ID set is empty")
		}
		version = ver
		var currIDs *IDSet
		if len(currData) == 0 {
			return -1, errors.New(fmt.Sprintf("no data for category '%s'", category))
		} else {
			// deserialize data
			currIDs, err = IDSetFromString(string(currData))
			if err != nil {
				return -1, err
			}
		}
		// push IDs
		if err = currIDs.PushIDsFromString(idSet.String()); err != nil {
			return -1, err
		}
		// try to save data
		setStr := idSet.String()
		size := idSet.GetSize()
		stateStr := currIDs.String()
		if errFin = g.idProvider.SetData(stateStr, category, version); errFin != nil {
			log.Println("error saving data", errFin)
		} else {
			success = true
			log.Println("pushed ID set back to provider = ", "SET: ", setStr, "SIZE: ", size, "STATE: ", stateStr)
		}
	}
	if !success {
		errMsg := "failed to push IDs"
		log.Println(errMsg, errFin)
		return -1, errors.New(fmt.Sprintf(errMsg+": %s", errFin))
	}
	return version, nil
}

func (g *IDGenerator) Stop() int32 {
	log.Println("pushing back unused IDs ...")
	var version int32
	var err error
	for c := range g.idSets {
		version, err = g.PushIDsWithRetry(c)
		if err != nil {
			log.Println("error pushing sets", err)
		}
	}
	return version
}

func (g *IDGenerator) PeekIDs(category string) (*IDSet, error) {
	data, _, err := g.idProvider.GetData(category)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	currIDs, err := IDSetFromString(string(data))
	if err != nil {
		return nil, err
	}
	currIDs.SetReadOnly(true)
	return currIDs, err
}

type idReq struct {
	category string
	resp     chan idResp
}

type idResp struct {
	id  uint64
	err error
}

func (g *IDGenerator) takeIDHandler() {
	for {
		req := <-g.idReqChan

		var set *IDSet
		var err error
		var rsp idResp
		set, ok := g.idSets[req.category]
		if !ok || set.GetSize() == 0 {
			_, err = g.TakeIDsWithRetry(req.category)
			if err != nil {
				rsp.err = err
			}
			set = g.idSets[req.category]
		}
		if set != nil {
			id, err := set.TakeID()
			rsp.id = id
			rsp.err = err
		}

		req.resp <- rsp
	}
}

func (g *IDGenerator) TakeID(category string) (uint64, error) {
	req := idReq{
		category: category,
		resp:     make(chan idResp),
	}
	g.idReqChan <- req
	resp := <-req.resp
	return resp.id, resp.err
}
