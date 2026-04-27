# How topic.messagePump Sends Messages (Per Topic)

This document checks the real behavior of `Topic.messagePump` in [nsqd/topic.go](nsqd/topic.go).

## 1. Where messagePump starts

- `NewTopic` starts `messagePump` in its own goroutine immediately (`t.waitGroup.Wrap(t.messagePump)`).
- But the pump does not send messages yet. It blocks until `Start()` pushes to `startChan`.
- `Start()` is called after topic/channel setup in [nsqd/nsqd.go](nsqd/nsqd.go), so fan-out starts only when initialization is ready.

## 2. Startup gate behavior (before main loop)

Before start signal arrives, `messagePump` consumes only control events:

- `channelUpdateChan`: ignored and loop continues.
- `pauseChan`: ignored and loop continues.
- `exitChan`: exits immediately.
- `startChan`: breaks startup wait and enters normal operation.

Why this matters:

- The pump avoids delivering messages too early.
- Pause/channel updates are non-blocking even before the topic is fully started.

## 3. Initial channel snapshot and source activation

After start:

- It snapshots current channels from `channelMap` under read lock.
- It enables input sources only if there is at least one channel and topic is not paused:
  - memory source: `memoryMsgChan`
  - disk source: `backend.ReadChan()`
- If no channels or paused, both sources are set to `nil` (disabled in `select`).

## 4. Main dispatch loop

In steady state, `messagePump` selects from:

- `memoryMsgChan`: in-memory messages.
- `backendChan`: disk-backed messages (decoded with `decodeMessage`).
- `channelUpdateChan`: resnapshot channels and re-evaluate source enablement.
- `pauseChan`: re-evaluate source enablement.
- `exitChan`: stop loop.

Important detail:

- A decode failure from backend does not stop the pump; it logs error and continues.

## 5. Per-message fan-out to channels

For each message from memory or backend, the pump iterates all channels:

- First channel reuses the original message object.
- Every additional channel gets a cloned message object (`NewMessage` + copy timestamp + deferred duration).
- If message is deferred (`msg.deferred != 0`), it is sent via `channel.PutMessageDeferred`.
- Otherwise it is sent via `channel.PutMessage`.
- Per-channel send errors are logged; processing continues for other channels.

Consequence:

- Topic fan-out is best-effort per channel within one loop iteration; one failing channel does not block other channel deliveries.

## 6. Pause and channel-change semantics

Two control paths can disable/enable dispatch sources at runtime:

1. Channel update (`channelUpdateChan`)
- Rebuilds the channel slice from `channelMap`.
- If channel count is zero or topic paused: disable memory/backend inputs.
- Else: enable memory/backend inputs.

2. Pause update (`pauseChan`)
- Uses current channel slice.
- Applies same rule: no channels or paused -> disable inputs; else enable.

Effect:

- Messages can still be queued into topic memory/backend while paused.
- `messagePump` simply stops draining and fan-out until unpaused.

## 7. Interaction with put path (why both memory and backend are read)

`Topic.put` in [nsqd/topic.go](nsqd/topic.go):

- Prefers memory queue when possible.
- Falls back to backend disk queue when memory is full (or memory disabled).

`messagePump` therefore drains both:

- memory for low-latency fast path
- backend for overflow/persisted backlog

## 8. Exit behavior

When `exitChan` closes:

- `messagePump` logs closure and returns.
- Topic exit waits for this goroutine (`t.waitGroup.Wait()`), guaranteeing clean stop ordering.

## 9. Practical correctness summary

`topic.messagePump` is a single-goroutine fan-out dispatcher per topic with these guarantees:

- startup ordering guard (`startChan`)
- dynamic channel membership handling
- pause-aware delivery gating
- mixed memory+disk source draining
- per-channel isolation on send errors
- orderly shutdown through `exitChan` + wait group

In short, a topic sends messages by continuously pulling from its own memory/disk queues and replicating each message to every active channel, while control channels (`channelUpdateChan`, `pauseChan`, `exitChan`) dynamically govern whether dispatch is active.