package shardMap

import (
	"fmt"
	"sync"
)

type ShardMap struct {
	origSize  int
	shardSize int
	shardMask int
	maps      []sync.Map
}
type hasher interface {
	Hash() int
}

func NewShardMap(n int) (*ShardMap, error) {
	if n == 0 {
		return nil, fmt.Errorf("Shards number must be > 0 ")
	}
	// if !IsPowerOfTwo(config.Shards) {
	// 	return nil, fmt.Errorf("Shards number must be power of two")
	// }
	sm := &ShardMap{}
	sm.origSize = n
	sm.shardSize = CeilToPowerOfTwo(n)
	sm.shardMask = sm.shardSize - 1
	sm.maps = make([]sync.Map, sm.shardSize)
	return sm, nil
}

func (sm *ShardMap) Load(key hasher) (interface{}, bool) {
	hash := key.Hash()
	shardId := hash & sm.shardMask
	return sm.maps[shardId].Load(key)
}

func (sm *ShardMap) Store(key hasher, v interface{}) {
	hash := key.Hash()
	shardId := hash & sm.shardMask
	sm.maps[shardId].Store(key, v)
}

