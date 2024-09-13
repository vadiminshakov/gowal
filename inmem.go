package gowal

import (
	"github.com/vadiminshakov/gowal/msg"
	"sync"
)

type InmemWalMock struct {
	kv      map[uint64]msg.Msg
	muKv    sync.RWMutex
	muVotes sync.RWMutex
}

func NewInmemWalMock() *InmemWalMock {
	kvstore := make(map[uint64]msg.Msg)
	return &InmemWalMock{kv: kvstore}
}

func (c *InmemWalMock) Set(index uint64, key string, value []byte) error {
	c.muKv.Lock()
	defer c.muKv.Unlock()
	c.kv[index] = msg.Msg{Key: key, Value: value, Idx: index}

	return nil
}

func (c *InmemWalMock) Get(index uint64) (string, []byte, bool) {
	c.muKv.RLock()
	defer c.muKv.RUnlock()
	message, ok := c.kv[index]
	return message.Key, message.Value, ok
}

func (c *InmemWalMock) Close() error {
	return nil
}
