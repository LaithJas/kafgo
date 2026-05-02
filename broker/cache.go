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

func (c *cache) store(topic string, msg *message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.storage[topic]
	if !ok {
		return fmt.Errorf("topic %s does not exist", topic)
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
		return fmt.Errorf("topic %s already exist", topic)
	}
	c.storage[topic] = list.New()

	return nil
}

func (c *cache) retrieve(topic string) (*message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.storage[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}
	element := c.storage[topic].Front()
	if element == nil {
		return nil, fmt.Errorf("no messages in %s topic", topic)
	}
	data, ok := element.Value.(*message)
	if !ok {
		return nil, fmt.Errorf("list element is not of type message")
	}

	return data, nil
}

func ack(topic string, id uuid.UUID) error {
	return nil
}
