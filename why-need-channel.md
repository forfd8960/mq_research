# Why NSQ Needs Channel

This document consolidates the earlier explanations about:

1. what is the core concept/core object in NSQ,
2. why `Topic` needs a `channelMap` (instead of only topic + clients),
3. ASCII diagrams for flow and semantics.

---

## 1) Core concept: who drives lifecycle?

There are two useful perspectives:

- Process lifecycle orchestrator: `NSQD`
- Message flow semantic core: `Topic` (with `Channel` as the consumption boundary)

In practice:

- `NSQD` creates/manages topics, runs queue scanning workers, and hosts protocol/TCP entry.
- `Topic` is where published messages enter and then fan out to channels.
- `Channel` is the unit that owns delivery state (in-flight/deferred/requeue/timeout) for a consumer group.

So:

- If asking "single object driving full system lifecycle": **NSQD**.
- If asking "single object at center of message pipeline": **Topic**, but it must work together with **Channel**.

---

## 2) Why topic cannot be only "memory queue + map of clients"

Because NSQ semantic model is:

- **Topic = publish stream name**
- **Channel = independent consumer group of that stream**
- **Client = one subscriber connection within one channel**

If Topic directly held clients only, NSQ would lose consumer-group semantics.

### 2.1 Channel is not optional, it is the consumer-group boundary

Different channels under the same topic each need a full copy of the stream.

Example:

- topic: `orders`
- channels: `billing`, `shipping`, `analytics`

One published message must be consumed by all three groups, not only one global client set.

### 2.2 Reliability state must be isolated per group

ACK/requeue/timeout/deferred are channel-specific states.

The same logical message can be:

- finished in one channel,
- timed out and retried in another channel,
- deferred in a third channel.

Without channels, these states would interfere with each other.

### 2.3 Load-balancing and fanout need two levels

NSQ delivery is two-level:

1. Topic -> fanout to every channel
2. Inside each channel -> load-balance to clients via RDY/in-flight

If only topic + clients, you can only do one-level delivery and lose independent groups.

### 2.4 Discovery/pre-create mechanism is channel-oriented

`nsqd` can pre-create channels from lookupd metadata to preserve delivery semantics even before consumers connect.

That behavior depends on channels being first-class objects.

---

## 3) Key code anchors (for verification)

- `nsqd/topic.go`
  - `Topic.GetChannel()`
  - `Topic.messagePump()` fanout loop to all channels
- `nsqd/protocol_v2.go`
  - `SUB(topic, channel)` explicitly binds a client to a specific channel
- `nsqd/channel.go`
  - `FinishMessage`, `RequeueMessage`, `StartInFlightTimeout`, deferred/in-flight queues
- `nsqd/nsqd.go`
  - `GetTopic()` pre-creates channels from lookupd
  - `queueScanLoop()` drives timeout/deferred requeue processing

---

## 4) ASCII diagrams

### 4.1 Actual NSQ model (Topic + multiple Channels + multiple Clients)

```text
Producer
   |
   v
Topic: orders
   |
   | fanout one copy to each channel
   +---------------------------> Channel: billing
   |                               |
   |                               +--> Client B1
   |                               +--> Client B2
   |
   +---------------------------> Channel: shipping
   |                               |
   |                               +--> Client S1
   |
   +---------------------------> Channel: analytics
                                   |
                                   +--> Client A1
                                   +--> Client A2
                                   +--> Client A3
```

Meaning:

- each channel gets its own stream copy,
- each channel independently balances among its own clients.

### 4.2 What "Topic + clients only" would look like (and why wrong for NSQ)

```text
Producer
   |
   v
Topic: orders
   |
   +--> Client B1
   +--> Client S1
   +--> Client A1
   +--> Client A2
```

Problem:

- cannot represent independent consumer groups,
- cannot isolate ack/requeue/timeout states per group,
- cannot support true "each group consumes full stream" semantics.

### 4.3 Same message, different channel states

```text
Message M100 published to topic: orders
   |
   +--> billing channel   : in-flight -> FIN -> done
   |
   +--> shipping channel  : in-flight -> timeout -> REQUEUE -> redeliver
   |
   `--> analytics channel : deferred 5s -> eligible -> deliver -> FIN
```

This isolation is exactly why channels are required.

### 4.4 RDY / in-flight / FIN-REQ timeout sequence (single channel)

```text
Consumer C (SUB topic=orders channel=billing)
    |
    | RDY 1
    v
protocolV2.messagePump (for C)
    |
    | pull msg from billing channel queue/backend
    | StartInFlightTimeout(msg, C)
    | SendMessage(msg)
    v
Consumer receives msg
    |
    +-- FIN msgID --------------------------> Channel.FinishMessage
    |                                         (remove from in-flight, done)
    |
    +-- REQ msgID 0ms ---------------------> Channel.RequeueMessage
    |                                         (immediate requeue)
    |
    +-- REQ msgID 5000ms ------------------> Channel.RequeueMessage
    |                                         (deferred requeue)
    |
    `-- no FIN/REQ before timeout ---------> queueScanLoop/processInFlightQueue
                                              requeue for redelivery
```

---

## 5) Final takeaway

`channelMap` in `Topic` is not redundant; it is the structure that enables NSQ's core semantics:

- one publish stream,
- multiple independent consumer groups,
- reliable at-least-once delivery with per-group state isolation.

Without channels, NSQ would become a different system model and lose its key guarantees.
