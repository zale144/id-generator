package id_generator

type IDSet struct {
	getSizeCh     chan getSizeReq
	takeIDsCh     chan takeIDsReq
	takeIDCh      chan takeIDReq
	pushIDsCh     chan pushIDsReq
	setReadOnlyCh chan setReadOnlyReq
	isReadOnlyCh  chan isReadOnlyReq
	toStringCh    chan toStringReq
}

func NewIDSet(ranges []IDRange, category string, readOnly bool) *IDSet {
	idSet := IDSet{
		getSizeCh:     make(chan getSizeReq),
		takeIDsCh:     make(chan takeIDsReq),
		takeIDCh:      make(chan takeIDReq),
		pushIDsCh:     make(chan pushIDsReq),
		setReadOnlyCh: make(chan setReadOnlyReq),
		isReadOnlyCh:  make(chan isReadOnlyReq),
		toStringCh:    make(chan toStringReq),
	}
	go idSet.idSetCache(ranges, category, readOnly)
	return &idSet
}

func NewSet(idR idSet) *IDSet {
	return NewIDSet(idR.Ranges, idR.Category, idR.readOnly)
}

func IDSetFromString(jsn string) (*IDSet, error) {
	set, err := setFromString(jsn)
	if err != nil {
		return nil, err
	}
	return NewSet(set), nil
}

func (id IDSet) GetSize() uint64 {
	req := getSizeReq{
		resp: make(chan uint64),
	}
	id.getSizeCh <- req
	return <-req.resp
}

func (id IDSet) TakeIDs(idRangeSize uint64) (*IDSet, error) {
	req := takeIDsReq{
		size: idRangeSize,
		resp: make(chan rangeRespErr),
	}
	id.takeIDsCh <- req
	resp := <-req.resp
	return NewSet(resp.rang), resp.err
}

func (id IDSet) TakeID() (uint64, error) {
	req := takeIDReq{
		resp: make(chan idRespErr),
	}
	id.takeIDCh <- req
	resp := <-req.resp
	return resp.id, resp.err
}

func (id IDSet) PushIDsFromString(data string) error {
	req := pushIDsReq{
		data: data,
		resp: make(chan error),
	}
	id.pushIDsCh <- req
	return <-req.resp
}

func (id *IDSet) SetReadOnly(r bool) {
	req := setReadOnlyReq{
		readOnly: r,
	}
	id.setReadOnlyCh <- req
}

func (id IDSet) String() string {
	req := toStringReq{
		resp: make(chan string),
	}
	id.toStringCh <- req
	return <-req.resp
}

func (id IDSet) IsReadOnly() bool {
	req := isReadOnlyReq{
		resp: make(chan bool),
	}
	id.isReadOnlyCh <- req
	return <-req.resp
}

type rangeRespErr struct {
	rang idSet
	err  error
}

type idRespErr struct {
	id  uint64
	err error
}

type getSizeReq struct {
	resp chan uint64
}

type takeIDsReq struct {
	size uint64
	resp chan rangeRespErr
}

type takeIDReq struct {
	resp chan idRespErr
}

type pushIDsReq struct {
	data string
	resp chan error
}

type setReadOnlyReq struct {
	readOnly bool
}

type isReadOnlyReq struct {
	resp chan bool
}

type toStringReq struct {
	resp chan string
}

func (id *IDSet) idSetCache(ranges []IDRange, category string, readOnly bool) {
	rang := idSet{
		Ranges:   ranges,
		Category: category,
		readOnly: readOnly,
	}
	for {
		select {
		case gs := <-id.getSizeCh:
			gs.resp <- rang.getSize()
		case tIDs := <-id.takeIDsCh:
			rang, err := rang.takeIDs(tIDs.size)
			tIDs.resp <- rangeRespErr{rang: rang, err: err}
		case tID := <-id.takeIDCh:
			id, err := rang.takeID()
			tID.resp <- idRespErr{id: id, err: err}
		case pIDs := <-id.pushIDsCh:
			set, err := setFromString(pIDs.data)
			if err != nil {
				pIDs.resp <- err
			} else {
				pIDs.resp <- rang.pushIDs(set)
			}
		case ro := <-id.setReadOnlyCh:
			rang.readOnly = ro.readOnly
		case str := <-id.toStringCh:
			str.resp <- rang.toString()
		case iro := <-id.isReadOnlyCh:
			iro.resp <- rang.readOnly
		}
	}
}
