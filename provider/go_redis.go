package provider

/*
import (
	"errors"
	"fmt"
	lock "github.com/bsm/redis-lock"
	"github.com/go-redis/redis"
)

type RedisIDProvider struct {
	client *redis.Client
}

func NewRedisIDProvider(addr, pass string, db int) *RedisIDProvider {
	client := redis.NewClient(&redis.Options{
		Network:  "tcp",
		Addr:     addr,
		Password: pass,
		DB:       db,
	})

	provider := &RedisIDProvider{
		client: client,
	}
	return provider
}

func (r *RedisIDProvider) Initialize(initSetData string, category string) error {
	if initSetData == "" {
		return errors.New("no data provided")
	}
	_, err := r.client.Ping().Result()
	if err != nil {
		return err
	}
	return r.SetData(initSetData, category, -1)
}

func (r *RedisIDProvider) SetData(data, category string, version int32) error {
	err := r.client.Set(category, data, 0)
	if err != nil {
		return err.Err()
	}
	return nil
}

func (r *RedisIDProvider) GetData(category string) (string, int32, error) {
	result, err := r.client.Get(category).Result()
	if err != nil {
		return "", -1, map[bool]error{true:err}[err != redis.Nil]
	}
	return result, -1, nil
}

func (r *RedisIDProvider) Delete(category string, version int32) error {
	err := r.client.Del(category)
	if err != nil {
		return err.Err()
	}
	return nil
}

func (r *RedisIDProvider) Lock(category string) (interface{}, error) {
	lck, err := lock.Obtain(r.client, "lock."+category, nil)
	if err != nil {
		return nil, err
	} else if lck == nil {
		fmt.Println()
		return nil, errors.New("ERROR: could not obtain lock")
	}
	return lck, nil
}

func (r *RedisIDProvider) Unlock(l interface{}) error {
	return l.(*lock.Locker).Unlock()
}*/
