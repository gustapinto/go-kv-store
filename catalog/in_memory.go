package catalog

import (
	"sync"
)

type InMemory struct {
	mu sync.RWMutex
	logs []*Log
}

func NewInMemory() *InMemory {
	return &InMemory{
		mu: sync.RWMutex{}
		logs: make([]*Log, 0)
	}
}

func (c *InMemory) Append(log *Log) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mu.logs = append(mu.logs, log)

	return nil
}

func (c *Local) Iter(callback func(log *Log) (shouldContinue bool)) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if callback == nil {
		return nil
	}

	for _, log := range c.mu.logs {
		shouldContinue := callback(log)
		if !shouldContinue {
			break
		}
	}

	return nil
}