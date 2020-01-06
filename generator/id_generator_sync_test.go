package generator

import (
	"git.fxclub.org/wallet/id-generator/domain"
	"git.fxclub.org/wallet/id-generator/provider"
	"sync"
	"testing"
)

func Test_idGenerator_TakeIDMock(t *testing.T) {
	const take = 1000
	wantSize := defaultTotalSize - take

	// create and initialize id-generator
	mockIDProvider := provider.NewMockIDProvider()
	g := NewIDGenerator(mockIDProvider)
	if err := g.Initialize(domain.OperationIdCategory, 1); err != nil {
		t.Errorf("IDGenerator.Initialize() error = %v", err)
		return
	}

	wg := sync.WaitGroup{}
	for i := 0; i < take; i++ {
		wg.Add(1)
		go func(gen *IDGenerator) {
			defer wg.Done()
			_, err := gen.TakeID(domain.OperationIdCategory)
			if err != nil {
				t.Errorf("IDGenerator.TakeID() error = %v", err)
				return
			}
			//t.Logf("Got ID: %d", got)
		}(g)
	}
	wg.Wait()
	g.Stop()
	ids, err := g.PeekIDs(domain.OperationIdCategory)
	if err != nil {
		t.Errorf("IDGenerator.PeekIDs() error = %v", err)
		return
	}
	size := ids.GetSize()
	if wantSize != size {
		t.Errorf("IDGenerator.sizeAfter = %v, want %v", size, wantSize)
	}
	t.Logf("STATE AFTER: %s", ids.String())
}

func Test_idGenerator_TakeIDZooKeeper(t *testing.T) {

	// create and initialize id-generator
	zookeeperIDProvider, err := provider.NewZooKeeperIDProvider("rnd-r-kafka-0-node-ec2.rnd-env.com:2181")
	if err != nil {
		t.Errorf("provider.NewZooKeeperIDProvider error = %v", err)
		return
	}
	var lastVersion int32
	defer func() {
		if err := zookeeperIDProvider.Delete(domain.OperationIdCategory, lastVersion); err != nil {
			t.Errorf("zookeeperIDProvider.Delete error = %v", err)
			return
		}
	}()
	g := NewIDGenerator(zookeeperIDProvider)
	if err := g.Initialize(domain.OperationIdCategory, 1); err != nil {
		t.Errorf("IDGenerator.Initialize() error = %v", err)
		return
	}
	set, err := g.PeekIDs(domain.OperationIdCategory)
	if err != nil {
		t.Errorf("IDGenerator.PeekIDs() error = %v", err)
		return
	}
	initialSize := set.GetSize()
	const noGoroutines = 10
	const take = 50
	wantSize := initialSize - noGoroutines*take

	wg := sync.WaitGroup{}
	for i := 0; i < noGoroutines; i++ {
		wg.Add(1)
		go func(rp IDProvider, in int, ver *int32) {
			ig := NewIDGenerator(rp)
			defer func() {
				*ver = ig.Stop()
				wg.Done()
			}()

			for j := 0; j < take; j++ {
				_, err := ig.TakeID(domain.OperationIdCategory)
				if err != nil {
					t.Errorf("IDGenerator.TakeID() error = %v", err)
					return
				}
				//t.Logf("g-%d: Got ID: %d", in, got)
			}
		}(zookeeperIDProvider, i, &lastVersion)
	}
	wg.Wait()

	ids, err := g.PeekIDs(domain.OperationIdCategory)
	if err != nil {
		t.Errorf("IDGenerator.PeekIDs() error = %v", err)
		return
	}
	size := ids.GetSize()
	if wantSize != size {
		t.Errorf("IDGenerator.sizeAfter = %v, want %v", size, wantSize)
	}
	t.Logf("STATE AFTER: %s", ids.String())
}

func Test_idGenerator_TakeIDRedis(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping this since Redis is not being used yet")
	}

	// create and initialize id-generator
	redisIDProvider := provider.NewRedisIDProvider(":6379", "", 0)
	defer func() {
		if err := redisIDProvider.Delete(domain.OperationIdCategory, 0); err != nil {
			panic(err)
		}
	}()
	g := NewIDGenerator(redisIDProvider)
	if err := g.Initialize(domain.OperationIdCategory, 1); err != nil {
		t.Errorf("IDGenerator.Initialize() error = %v", err)
		return
	}
	set, err := g.PeekIDs(domain.OperationIdCategory)
	if err != nil {
		t.Errorf("IDGenerator.PeekIDs() error = %v", err)
		return
	}
	initialSize := set.GetSize()
	const noGoroutines = 10
	const take = 50
	wantSize := initialSize - noGoroutines*take

	wg := sync.WaitGroup{}
	for i := 0; i < noGoroutines; i++ {
		wg.Add(1)
		go func(rp IDProvider, in int) {
			defer wg.Done()
			ig := NewIDGenerator(rp)

			for j := 0; j < take; j++ {
				_, err := ig.TakeID(domain.OperationIdCategory)
				if err != nil {
					t.Errorf("IDGenerator.TakeID() error = %v", err)
					return
				}
				//t.Logf("g-%d: Got ID: %d", in, got)
			}
			ig.Stop()
		}(redisIDProvider, i)
	}
	wg.Wait()
	ids, err := g.PeekIDs(domain.OperationIdCategory)
	if err != nil {
		t.Errorf("IDGenerator.PeekIDs() error = %v", err)
		return
	}
	size := ids.GetSize()
	if wantSize != size {
		t.Errorf("IDGenerator.sizeAfter = %v, want %v", size, wantSize)
	}
	t.Logf("STATE AFTER: %s", ids.String())
}
