# Kafgo
## The intend of this project is for educational purposes to learn the Go programming language. The main idea is to get a working MQ that works in different scenarios. This could mean that some best practices that are usually found in MQs might not be implemented. This is a best effort with less focus on how MQs actually work and more focus on how a project can be completed using only Go programming language.

### What defines a Message Queue
main functionality of a message queue(MQ) is being able to Queue messages, objects, or any defined entity until it is processed.

MQs usually consist of 3 main parts:
- Producers: a process that's producing the data
- Data Broker: where the data is getting queued before its used
- Consumers: a process that consumes data

### Data Flow

Data in MQ usually flows from producers, to brokers, and then it's consumed by consumers. All these parts are separated from each other. That means they don't know if data is getting produced or consumed. The broker is where data stays until it's consumed.

## Implementation thought
1. Work on designing and implementing the data broker.
    - all data should be stored as generic type so we can accept and anything
    - initial implementation should focus on using interfaces to implement code for ease of change
    - use concurrency to process incoming and outgoing connections
    - use authentication that producers and consumers must use before being able to connect to the broker
    - use a normal queue (not a priority queue) for now since we're going for simplicity
2. designing producers
3. designing consumers

This is an initial draft initial thoughts on this. Changes gonna happen later
