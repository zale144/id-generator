package provider

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"time"
)

type ZooKeeperIDProvider struct {
	client *zk.Conn
}

func NewZooKeeperIDProvider(addr string) (*ZooKeeperIDProvider, error) {
	c, session, err := zk.Connect([]string{addr}, time.Second)
	if err != nil {
		return nil, err
	}
	for event := range session {
		if event.State == zk.StateConnected {
			log.Printf("zookeeper State: %s\n", event.State)
			break
		}
	}
	return &ZooKeeperIDProvider{
		client: c,
	}, nil
}

func (r *ZooKeeperIDProvider) Initialize(initSetData string, category string) error {
	if initSetData == "" {
		return errors.New("no data provided")
	}
	exists, stat, err := r.client.Exists("/" + category)
	if err != nil {
		return err
	}

	if !exists {
		_, err = r.client.Create("/"+category, []byte(initSetData), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	} else if stat.Version == 0 {
		err = r.SetData(initSetData, category, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ZooKeeperIDProvider) SetData(data, category string, version int32) error {
	_, err := r.client.Set("/"+category, []byte(data), version)
	if err != nil {
		return err
	}
	return nil
}

func (r *ZooKeeperIDProvider) GetData(category string) (string, int32, error) {
	result, stat, err := r.client.Get("/" + category)
	if err != nil {
		return "", -1, map[bool]error{true: err}[err != nil]
	}
	return string(result), stat.Version, nil
}

func (r *ZooKeeperIDProvider) Delete(category string, version int32) error {
	err := r.client.Delete("/"+category, version)
	if err != nil {
		return err
	}
	return nil
}

func (r *ZooKeeperIDProvider) Lock(string) (interface{}, error) {
	return nil, nil
}

func (r *ZooKeeperIDProvider) Unlock(lck interface{}) error {
	return nil
}
