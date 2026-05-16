// Package broker has definitions and functinallity of the broker
// the pakcage is split into a cache file and service file. cache
// will handle storage management and correlation of message
// while service is going to handle connecitons and requests fromt
// consumer and producers
package broker

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type cache struct {
	storage      map[string]*list.List // each topic has a list of messages as topic: messages
	subscribers  map[string][]string   // contains who subscribed to the borker as topic: []subscribers
	mu           sync.RWMutex
	receivedMsgs int // number of received Msgs
	ackedMsgs    int // number of acked messages
}

type message struct {
	id         uuid.UUID
	topic      string
	data       any
	receivedAt time.Time
	ackedAt    time.Time
}

// newCache initializes a new cache whenever called
func newCache() *cache {
	return &cache{
		storage:     make(map[string]*list.List),
		subscribers: make(map[string][]string),
	}
}

// subscribe takes a topic name and consumer name as params and
// subscribe that consumner to the topic
// it will returns an error if topic doesn't exist
// TODO: return an error if consumebr is alredy subscribed to the same topic
func (c *cache) subscribe(topic, consumer string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.storage[topic]
	if !ok {
		return fmt.Errorf("CAS0001AN_%s", topic)
	}
	c.subscribers[topic] = append(c.subscribers[topic], consumer)

	return nil
}

// store takes a topic and a message and stores that message
// in that specific topic's queue until it's consumed
// it will returns an error if topic doesn't exist
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

// create takes a topic name and it will add that topic list of caches
// that consumers can consume from
// it will return an error if the topic already exits
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

// retrieve takes a topic and retrieves the first message that was
// sent to that topic (FIFO pattern)
// it will return an error if:
// - topic doesn't exits
// - message doesn't exits
// - if the list element is not of type message
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

// ack takes a topic and message ID and acknowledge that the message
// got received by the consumer and ready to be dropped from the queue
// it will return an error if topic doesn't exists
// or if the list emeent is not of type message
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
