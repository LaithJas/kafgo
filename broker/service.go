package broker

import (
	"net/http"
)

// first open a service that cosumers can connect to send their data
// service should be concurrent and can handle multiple requests
//
// for the queue data structre we're going to use the container/list package
// for protecting the concuurent process we use sync.RWMutex
// to generate universally unique IDs we use google/uuid
// for Observability we'd need to track:
// - total message recieived
// - total message package
// - current queue size per topic
//
//need multiple topics that are mapped in a map.
// the cache sturct storage field would be a map where:
// - topic name is the key
// - value is the message list
