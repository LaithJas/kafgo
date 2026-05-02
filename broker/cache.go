package broker

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type cache struct {
	storage      map[string]*list.List
	mu           sync.RWMutex
	receivedMsgs int
	ackedMsgs    int
}

type message struct {
	id         uuid.UUID
	topic      string
	data       any
	receivedAt time.Time
	ackedAt    time.Time
}

func newCache() *cache {
	return &cache{
		storage: make(map[string]*list.List),
	}
}

func (c *cache) store(topic string, msg *message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.storage[topic]
	if !ok {
		return fmt.Errorf("CAS0001AN_%s", topic)
	}
	c.storage[topic].PushBack(msg)
	c.receivedMsgs++
	return nil
}

func (c *cache) create(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.storage[topic]
	if ok {
		return fmt.Errorf("CAS002AN_%s", topic)
	}
	c.storage[topic] = list.New()

	return nil
}

func (c *cache) retrieve(topic string) (*message, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.storage[topic]
	if !ok {
		return nil, fmt.Errorf("CAS001AN_%s", topic)
	}
	element := c.storage[topic].Front()
	if element == nil {
		return nil, fmt.Errorf("CAS003AN_%s", topic)
	}
	data, ok := element.Value.(*message)
	if !ok {
		return nil, fmt.Errorf("CAS004AM")
	}

	return data, nil
}

func (c *cache) ack(topic string, id uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.storage[topic]
	if !ok {
		return fmt.Errorf("CAS001AN_%s", topic)
	}
	// find the element with UUID == id
	for e := c.storage[topic].Front(); e != nil; e = e.Next() {
		data, ok := e.Value.(*message)
		if !ok {
			return fmt.Errorf("CAS004AM")
		}
		if data.id == id {
			data.ackedAt = time.Now()
			c.storage[topic].Remove(e)
			c.ackedMsgs++
			return nil
		}
	}
	return fmt.Errorf("CAS005AM")
}
