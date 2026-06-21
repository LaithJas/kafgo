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
	"github.com/laithjas/kafgo/consumer"
)

type cache struct {
	storage      map[string]*list.List              // each topic has a list of messages as topic: messages
	subscribers  map[string][]uuid.UUID             // contains who subscribed to the borker as topic: []subscribers
	msgOffset    map[uuid.UUID]map[string]uuid.UUID // message offset for each consumer per topic
	mu           sync.RWMutex
	receivedMsgs int // number of received Msgs
	ackedMsgs    int // number of acked messages
}

type message struct {
	id             uuid.UUID
	topic          string
	data           any
	receivedAt     time.Time
	ackedAt        time.Time
	ackedConsumers map[uuid.UUID]bool
}

// newCache initializes a new cache whenever called
func newCache() *cache {
	return &cache{
		storage:     make(map[string]*list.List),
		subscribers: make(map[string][]uuid.UUID),
		msgOffset:   make(map[uuid.UUID]map[string]uuid.UUID),
	}
}

// subscribe takes a topic name and consumer name as params and
// subscribe that consumner to the topic
// it will returns an error if topic doesn't exist
// TODO: return an error if consumebr is alredy subscribed to the same topic
func (c *cache) subscribe(topic string, consumer uuid.UUID) error {
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
func (c *cache) retrieve(topic string, consumerID uuid.UUID) (*message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.storage[topic]
	if !ok {
		return nil, fmt.Errorf("CAS001AN_%s", topic)
	}

	var msg *message

	// if this is the first message being retrieved, then there's no offset yet. set it up
	consumerMap, ok := c.msgOffset[consumerID] // returns a map that you can give a topic
	if !ok {
		tmpMsg, _ := c.storage[topic].Front().Value.(*message)
		c.msgOffset[consumerID] = map[string]uuid.UUID{topic: tmpMsg.id}
		return tmpMsg, nil
	}
	consumerOffsetMsgId, ok := consumerMap[topic] // returns the ID of a message
	if !ok {
		tmpMsg, _ := c.storage[topic].Front().Value.(*message)
		consumerMap[topic] = tmpMsg.id
		msg = tmpMsg
	} else {
		for e := c.storage[topic].Front(); e != nil; e = e.Next() {
			topicMsg, _ := e.Value.(*message)
			if consumerOffsetMsgId == topicMsg.id {
				if e.Next() == nil {
					return nil, fmt.Errorf("no message")
				}
				msg, ok = e.Next().Value.(*message)
				if !ok {
					return nil, fmt.Errorf("CAS004AM")
				}
				c.msgOffset[consumerID][topic] = msg.id
			}
		}
		if msg == nil {
			frontMsg, _ := c.storage[topic].Front().Value.(*message)
			msg = frontMsg
			c.msgOffset[consumerID][topic] = msg.id
		}
	}
	return msg, nil
}

// ack takes a topic and message ID and acknowledge that the message
// got received by the consumer and ready to be dropped from the queue
// it will return an error if topic doesn't exists
// or if the list emeent is not of type message
func (c *cache) ack(topic string, id, consumerID uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.storage[topic]
	if !ok {
		return fmt.Errorf("CAS001AN_%s", topic)
	}
	// find the element with UUID == id
	for e := c.storage[topic].Front(); e != nil; e = e.Next() {
		msg, ok := e.Value.(*message)
		if !ok {
			return fmt.Errorf("CAS004AM")
		}
		if msg.id == id {
			msg.ackedAt = time.Now()
			msg.ackedConsumers[consumerID] = true
			for _, cons := range c.subscribers[topic] {
				if !msg.ackedConsumers[cons] {
					return nil
				}
			}
			c.storage[topic].Remove(e)
			c.ackedMsgs++
			return nil
		}
	}
	return fmt.Errorf("CAS005AM")
}

// I have a stroage map with 			topic : list of message
// and I havea subscribers mpa with 	topic : list of subscribers
//
//we need to track subscribers and make sure all subscribers for a given topic
//has acked a message before we Remove(e)
