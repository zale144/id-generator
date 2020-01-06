package provider

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/redsync.v1"
	"time"
)

type RedisIDProvider struct {
	pool    *redis.Pool
	redsync *redsync.Redsync
}

func NewRedisIDProvider(addr, pass string, db int) *RedisIDProvider {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	pools := []redsync.Pool{pool}
	provider := &RedisIDProvider{
		pool:    pool,
		redsync: redsync.New(pools),
	}
	return provider
}

func (r *RedisIDProvider) Initialize(initSetData string, category string) error {
	if initSetData == "" {
		return errors.New("no data provided")
	}
	return r.SetData(initSetData, category, -1)
}

func (r *RedisIDProvider) GetData(category string) (data string, v int32, err error) {
	conn := r.pool.Get()
	defer func() {
		err = conn.Close()
	}()
	var bytes []byte
	bytes, err = redis.Bytes(conn.Do("GET", category))
	if err != nil {
		err = fmt.Errorf("error getting key %s: %v", category, err)
	}
	data = string(bytes)
	return
}

func (r *RedisIDProvider) SetData(data, category string, version int32) (err error) {
	conn := r.pool.Get()
	defer func() {
		err = conn.Close()
	}()
	_, err = conn.Do("SET", category, data)
	if err != nil {
		v := string(data)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error setting key %s to %s: %v", category, v, err)
	}
	return err
}

func (r *RedisIDProvider) Exists(key string) (e bool, err error) {
	conn := r.pool.Get()
	defer func() {
		err = conn.Close()
	}()
	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf("error checking if key %s exists: %v", key, err)
	}
	return ok, err
}

func (r *RedisIDProvider) Delete(category string, i int32) (err error) {
	conn := r.pool.Get()
	defer func() {
		err = conn.Close()
	}()
	_, err = conn.Do("DEL", category)
	return err
}

func (r *RedisIDProvider) Lock(category string) (interface{}, error) {
	mutex := r.redsync.NewMutex("lock." + category)
	err := mutex.Lock()
	if err != nil {
		return nil, err
	}
	return mutex, nil
}

func (r *RedisIDProvider) Unlock(lck interface{}) error {
	mutex := lck.(*redsync.Mutex)
	u := mutex.Unlock()
	if u {
		return errors.New("could not unlock")
	}
	return nil
}
