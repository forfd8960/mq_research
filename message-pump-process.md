# NSQ Message Pump Process (Function-Level)

This note extends the previous channel/topic explanation and focuses on the exact call path around message pump logic.

Scope:

1. Topic side message pump
2. Channel enqueue and state transitions
3. Client side protocol message pump
4. FIN/REQ/timeout feedback loop
5. Queue scan worker behavior

---

## 1) Big picture

NSQ has two pump layers:

1. Topic messagePump
   - reads from topic memory/backend
   - fans out to every channel

2. Protocol (per-client) messagePump
   - reads from one subscribed channel
   - sends to one client based on RDY and in-flight limits

Between them:

- Channel is the boundary holding delivery state (in-flight/deferred/timeout/requeue).

---

## 2) Function-level pipeline

Publish side to topic:

- protocolV2.PUB / MPUB / DPUB
- Topic.PutMessage / Topic.PutMessages
- Topic.put (memory first, backend fallback)

Topic fanout side:

- Topic.messagePump
  - read topic memoryMsgChan or topic backend.ReadChan
  - decodeMessage when source is backend bytes
  - for each channel in topic.channelMap:
    - clone for second and later channels
    - deferred -> Channel.PutMessageDeferred
    - normal -> Channel.PutMessage

Consumer delivery side:

- protocolV2.SUB binds client to one channel
- protocolV2.RDY sets client credit
- protocolV2.messagePump for that client:
  - only active when client.IsReadyForMessages is true
  - receive from channel memory/backend
  - msg.Attempts++
  - Channel.StartInFlightTimeout
  - client.SendingMessage
  - SendMessage(frameTypeMessage)

Feedback side:

- FIN -> Channel.FinishMessage -> remove in-flight
- REQ -> Channel.RequeueMessage -> immediate or deferred requeue
- no FIN/REQ before timeout -> queueScanLoop/queueScanWorker -> processInFlightQueue -> requeue

---

## 3) ASCII: end-to-end function sequence

```text
Producer
  |
  | PUB/MPUB/DPUB
  v
protocolV2.Exec
  |
  v
protocolV2.PUB/MPUB/DPUB
  |
  v
Topic.PutMessage(s)
  |
  +--> Topic.put -> topic.memoryMsgChan
  |
  `--> Topic.put -> writeMessageToBackend(topic.backend)


Topic goroutine: Topic.messagePump
  |
  +--> read topic.memoryMsgChan
  |      or
  `--> read topic.backend.ReadChan + decodeMessage
  |
  v
for channel in topic.channelMap:
  |
  +--> Channel.PutMessageDeferred (if msg.deferred != 0)
  |
  `--> Channel.PutMessage -> channel.put
          |
          +--> channel memory/topology chan
          `--> writeMessageToBackend(channel.backend)


Client goroutine: protocolV2.messagePump(client)
  |
  | waits SUB event + RDY credit
  |
  +--> read channel memory/backend
  +--> msg.Attempts++
  +--> Channel.StartInFlightTimeout(msg, clientID, msgTimeout)
  +--> client.SendingMessage()
  `--> SendMessage(client, msg)


Client response path
  |
  +--> FIN -> protocolV2.FIN -> Channel.FinishMessage -> done
  |
  +--> REQ -> protocolV2.REQ -> Channel.RequeueMessage
  |                              |-> timeout=0 immediate put
  |                              `-> timeout>0 StartDeferredTimeout
  |
  `--> no response -> NSQD.queueScanLoop -> queueScanWorker
                     -> Channel.processInFlightQueue -> put(msg)
```

---

## 4) ASCII: RDY and in-flight gating

```text
Consumer sends RDY N
  |
  v
client.ReadyCount = N

protocolV2.messagePump loop:
  if subChannel == nil or !client.IsReadyForMessages:
      do not select message channels
      flush buffered data if needed
  else:
      select message channels

When one message is selected:
  StartInFlightTimeout
  SendingMessage (InFlightCount +1)
  SendMessage

When FIN or REQ arrives:
  InFlightCount -1 (via FinishedMessage/RequeuedMessage)
  Ready state re-evaluated
```

Implication:

- RDY is flow-control credit.
- in-flight prevents over-delivery per client.

---

## 5) ASCII: timeout and deferred scanning

```text
NSQD.queueScanLoop (periodic)
  |
  +--> choose random subset of channels
  +--> dispatch to queueScanWorker pool

queueScanWorker(channel):
  |
  +--> channel.processInFlightQueue(now)
  |      - pop expired in-flight
  |      - notify client.TimedOutMessage if client exists
  |      - channel.put(msg) for redelivery
  |
  `--> channel.processDeferredQueue(now)
         - pop due deferred
         - channel.put(msg)
```

Implication:

- retry is not only explicit REQ, it is also automatic on timeout.
- deferred and timeout re-entry both converge into channel.put.

---

## 6) Why this two-pump design works

1. Topic pump handles fanout semantics once.
2. Channel stores per-group reliability state.
3. Client pump enforces per-connection flow control.
4. Queue scan handles eventual retry progress.

Together, they implement at-least-once delivery with independent channel behavior.
