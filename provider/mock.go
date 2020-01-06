package provider

import (
	"github.com/pkg/errors"
)

type MockIDProvider struct {
	getDataCh chan getDataReq
	setDataCh chan setDataReq
	delDataCh chan delDataReq
}

func NewMockIDProvider() *MockIDProvider {
	provider := MockIDProvider{
		getDataCh: make(chan getDataReq),
		setDataCh: make(chan setDataReq),
		delDataCh: make(chan delDataReq),
	}
	go provider.cache()
	return &provider
}

func (mp *MockIDProvider) Initialize(initData string, category string) error {
	if initData == "" {
		return errors.New("no data provided")
	}
	return mp.SetData(initData, category, 0)
}

type dataItem struct {
	raw     string
	version int32
}

func (mp *MockIDProvider) GetData(category string) (string, int32, error) {
	req := getDataReq{
		category: category,
		resp:     make(chan dataItem),
	}
	mp.getDataCh <- req
	d := <-req.resp
	return d.raw, d.version, nil
}

func (mp *MockIDProvider) SetData(data, category string, version int32) error {
	req := setDataReq{
		data: dataItem{
			raw:     data,
			version: version,
		},
		category: category,
		resp:     make(chan error),
	}
	mp.setDataCh <- req
	return <-req.resp
}

func (mp *MockIDProvider) Delete(category string, version int32) error {
	req := delDataReq{
		category: category,
		version:  version,
	}
	mp.delDataCh <- req
	return nil
}

func (mp *MockIDProvider) Lock(string) (interface{}, error) {
	return nil, nil
}

func (mp *MockIDProvider) Unlock(lck interface{}) error {
	return nil
}

type getDataReq struct {
	category string
	resp     chan dataItem
}

type setDataReq struct {
	data     dataItem
	category string
	resp     chan error
}

type delDataReq struct {
	category string
	version  int32
	resp     chan error
}

func (mp *MockIDProvider) cache() {
	m := make(map[string]dataItem)
	for {
		select {
		case gd := <-mp.getDataCh:
			gd.resp <- m[gd.category]
		case sd := <-mp.setDataCh:
			d := m[sd.category]
			if d.version >= 0 && sd.data.version != d.version {
				sd.resp <- errors.New("version doesn't match")
			} else {
				sd.data.version = sd.data.version + 1
				m[sd.category] = sd.data
				sd.resp <- nil
			}
		case d := <-mp.delDataCh:
			if d.version >= 0 && d.version != d.version {
				d.resp <- errors.New("version doesn't match")
			}
			delete(m, d.category)
			d.resp <- nil
		}
	}
}
