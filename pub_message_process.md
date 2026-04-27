# NSQ Message Publish and Delivery Process (nsqd)

This report explains how a message is:

1. received from a producer client,
2. put into nsqd queues,
3. delivered to consumer clients,
4. acknowledged, retried, or timed out.

The analysis is based on the code in `nsqd/`.

## 1) Entry: Producer connection and command parsing

Main TCP entry:

- `nsqd/tcp.go` -> `tcpServer.Handle(conn)`
- Reads 4-byte protocol magic (expects `"  V2"`)
- Creates `protocolV2` client and enters `IOLoop`

Protocol loop:

- `nsqd/protocol_v2.go` -> `IOLoop()`
- Reads line-based commands (`PUB`, `MPUB`, `DPUB`, `SUB`, `RDY`, `FIN`, `REQ`, `TOUCH`, ...)
- Dispatches via `Exec()`

## 2) Producer publish path (PUB/MPUB/DPUB)

### 2.1 PUB

`protocol_v2.go` -> `PUB()`:

- Validates topic name.
- Reads message body length and body bytes.
- Auth check (`CheckAuth`).
- Gets topic: `nsqd.GetTopic(topicName)`.
- Creates `Message` with topic-generated ID: `NewMessage(topic.GenerateID(), body)`.
- Enqueues: `topic.PutMessage(msg)`.

### 2.2 MPUB

`protocol_v2.go` -> `MPUB()` + `readMPUB()`:

- Validates a multi-message body.
- Creates one `Message` per item (each has unique generated ID).
- Batch enqueue via `topic.PutMessages(messages)`.

### 2.3 DPUB (deferred publish)

`protocol_v2.go` -> `DPUB()`:

- Like PUB, but parses defer timeout.
- Sets `msg.deferred = timeout`.
- Calls `topic.PutMessage(msg)`.

## 3) Topic queueing and fanout to channels

### 3.1 Topic creation and startup

`nsqd/nsqd.go` -> `GetTopic()`:

- Creates topic if absent.
- Topic message pump is started by `t.Start()` (after channel pre-create logic).

`nsqd/topic.go` -> `NewTopic()`:

- Builds in-memory queue `memoryMsgChan`.
- Builds backend disk queue (`diskqueue.New`) for non-ephemeral topics.
- Starts topic goroutine: `t.messagePump`.

### 3.2 Topic enqueue decision (memory vs disk)

`topic.go` -> `PutMessage()` / `PutMessages()` -> `put(m)`:

- If memory queue has space, push to `memoryMsgChan`.
- Else write serialized message to backend queue via:
  - `message.go` -> `writeMessageToBackend()` -> `BackendQueue.Put(...)`.
- For `deferred` messages, topic prefers memory path (so defer metadata is preserved before channel-level scheduling).

### 3.3 Topic fanout logic

`topic.go` -> `messagePump()`:

- Reads messages from:
  - topic memory queue (`memoryMsgChan`), or
  - topic backend queue (`backend.ReadChan()` + `decodeMessage`).
- Fanout to every channel in `channelMap`.
- For N channels, first channel may reuse original message object; others get cloned message structs.
- If message has `deferred`, calls `channel.PutMessageDeferred(...)`.
- Otherwise calls `channel.PutMessage(...)`.

This is where one topic message becomes per-channel message copies.

## 4) Channel buffering, deferred/in-flight state, and disk fallback

`nsqd/channel.go`:

- `PutMessage(m)` -> `put(m)`:
  - Enqueue to channel memory queues (and topology-aware queues if enabled), else
  - fallback to channel backend disk queue.
- `PutMessageDeferred(msg, timeout)` -> `StartDeferredTimeout(...)`:
  - puts message into deferred map + priority queue.

Channel tracks two important runtime structures:

- In-flight (sent to a client, waiting for `FIN`/`REQ`/timeout):
  - `inFlightMessages` map
  - `inFlightPQ`
- Deferred (not yet eligible for delivery):
  - `deferredMessages` map
  - `deferredPQ`

## 5) Consumer subscribe and receive path

### 5.1 SUB and RDY

`protocol_v2.go`:

- `SUB(topic, channel)`:
  - Gets topic and channel.
  - `channel.AddClient(clientID, client)`.
  - Sets client state to subscribed.
  - Sends channel to client pump through `client.SubEventChan`.
- `RDY(count)`:
  - Sets consumer credit (`client.ReadyCount`).

### 5.2 Per-client message pump (actual send)

`protocol_v2.go` -> `messagePump(client, ...)`:

- Runs one goroutine per connection.
- Only reads from channel queues when `client.IsReadyForMessages()` is true.
- Pulls message from channel memory queues or backend queue.
- Before sending:
  - increments `msg.Attempts`
  - calls `subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)`
  - updates client counters via `client.SendingMessage()`
- Sends framed payload with `SendMessage()` (`frameTypeMessage`).

So delivery is credit-based (`RDY`) and in-flight tracked before wire send completes.

## 6) ACK, requeue, touch, and timeout behavior

### 6.1 FIN (ack)

`protocol_v2.go` -> `FIN()` -> `channel.FinishMessage(clientID, msgID)`:

- Removes from in-flight map + PQ.
- Message is done (not requeued).

### 6.2 REQ (negative ack / retry)

`protocol_v2.go` -> `REQ()` -> `channel.RequeueMessage(...)`:

- Removes from in-flight first.
- If timeout == 0: immediate requeue through `channel.put(msg)`.
- If timeout > 0: deferred requeue via `StartDeferredTimeout`.

### 6.3 TOUCH (extend processing timeout)

`protocol_v2.go` -> `TOUCH()` -> `channel.TouchMessage(...)`:

- Recomputes in-flight expiry and updates in-flight PQ.

### 6.4 Automatic timeout/deferred processing

`nsqd/nsqd.go` -> `queueScanLoop()` + `queueScanWorker()`:

- Periodically scans random channels.
- Calls:
  - `channel.processInFlightQueue(now)`
  - `channel.processDeferredQueue(now)`

`channel.processInFlightQueue`:

- Pops expired in-flight messages.
- Notifies client timeout accounting (`client.TimedOutMessage()` if client still exists).
- Requeues message via `channel.put(msg)`.

`channel.processDeferredQueue`:

- Pops due deferred messages.
- Re-enqueues via `channel.put(msg)`.

## 7) Message serialization format

`nsqd/message.go`:

- On queue write / network send, message binary layout is:
  - 8 bytes timestamp
  - 2 bytes attempts
  - 16 bytes message ID
  - N bytes body

Disk backend stores this serialized form; backend readers decode with `decodeMessage`.

## 8) End-to-end ASCII sequence

### 8.1 Publish to queue

```text
Producer TCP Client
    |
    | "  V2" + PUB/MPUB/DPUB
    v
tcpServer.Handle -> protocolV2.IOLoop -> Exec
    |
    v
PUB/MPUB/DPUB handler
    |
    | create Message(id, body[, deferred])
    v
Topic.PutMessage / PutMessages
    |
    +--> topic memoryMsgChan (if available)
    |
    `--> topic backend diskqueue (fallback)
```

### 8.2 Topic fanout to channels

```text
Topic.messagePump
    |
    +-- read from topic memory chan
    |        or
    `-- read from topic backend
    |
    v
for each Channel in topic:
    - clone message for additional channels
    - deferred ? PutMessageDeferred : PutMessage
    |
    +--> channel memory/topology chans
    `--> channel backend diskqueue (fallback)
```

### 8.3 Channel to consumer and ack/retry

```text
Consumer SUB + RDY
    |
    v
protocolV2.messagePump (per client)
    |
    | read from channel queues/backend if RDY allows
    | StartInFlightTimeout(msg)
    | SendMessage(frameTypeMessage)
    v
Consumer receives message
    |
    +-- FIN  -> FinishMessage -> remove in-flight (done)
    |
    +-- REQ  -> RequeueMessage -> immediate/deferred requeue
    |
    `-- no response in time -> queueScanLoop timeout -> requeue
```

## 9) Key design points

- Durability and throughput are balanced by memory-first with disk fallback.
- Fanout happens at topic level; each channel has independent delivery state.
- Delivery to consumers is pull-credit style using `RDY`.
- At-least-once semantics are implemented by in-flight tracking + requeue on timeout/failure.
- Deferred delivery is modeled as priority-queue scheduling at channel level.
