# End-to-end: how a message reaches disk

NSQ persistence is a two-layer design:

1. NSQ message serialization and queueing decisions in this repository.
2. File-backed FIFO mechanics in dependency github.com/nsqio/go-diskqueue v1.1.0 (declared in [go.mod](go.mod)).

Publish path:

1. A message is created and encoded in [nsqd/message.go](nsqd/message.go).
2. Topic or Channel decides memory-first vs disk spill:
   - Topic path in [nsqd/topic.go](nsqd/topic.go)
   - Channel path in [nsqd/channel.go](nsqd/channel.go)
3. When disk is chosen, NSQ calls `writeMessageToBackend` in [nsqd/message.go](nsqd/message.go), which writes bytes to `BackendQueue.Put`.
4. `BackendQueue` is an interface in [nsqd/backend_queue.go](nsqd/backend_queue.go), and normal (non-ephemeral) topics/channels bind it to diskqueue.New in:
   - [nsqd/topic.go](nsqd/topic.go)
   - [nsqd/channel.go](nsqd/channel.go)

Important nuance:

- Ephemeral topic/channel uses dummy backend (no real disk persistence) via [nsqd/dummy_backend_queue.go](nsqd/dummy_backend_queue.go).
- Default durability tuning comes from [nsqd/options.go](nsqd/options.go): `MemQueueSize`, `MaxBytesPerFile`, `SyncEvery`, `SyncTimeout`.


How diskqueue itself works
Below is from the go-diskqueue source used by NSQ.

1) Core persisted state
diskqueue keeps and persists:

readFileNum, readPos
writeFileNum, writePos
depth
It stores these in a metadata file:

<name>.diskqueue.meta.dat
and data files:
<name>.diskqueue.000000.dat, 000001.dat, etc.
2) Single goroutine coordinator (ioLoop)
All reads/writes/sync/empty are serialized through one ioLoop goroutine and internal channels:

writeChan for Put
readChan/peekChan for consumer reads
depthChan for Depth
emptyChan for Empty
This avoids lock-heavy filesystem concurrency and makes ordering/state transitions deterministic.

3) Write path
Put sends data into writeChan; ioLoop handles writeOne:

validates message size against min/max
rolls file when current file would exceed MaxBytesPerFile
appends one length-prefixed record
increments writePos and depth
4) Read path
ioLoop decides if queue has data by comparing read and write pointers.
If so:

readOne reads one record from current read position
data is offered on readChan (or peekChan)
only when readChan case succeeds, moveForward commits read pointer and decrements depth
This “read then commit pointer advance” pattern ensures reads are stateful only after delivery from ioLoop.

5) File rolling and cleanup
Writer rolls to next file when max-bytes-per-file reached.
Reader rolls when finishing a complete file.
After moving to next read file, old file is removed.
6) Sync and metadata durability
Sync policy is hybrid:

count-based: every SyncEvery operations
time-based: every SyncTimeout if there was activity
sync() does:

fsync current write file
atomically persist metadata via tmp file + rename
This is the core durability boundary.

7) Restart recovery logic
On startup, retrieveMetaData restores pointers/depth from metadata.
If metadata writePos is behind actual file size (unclean shutdown risk), diskqueue advances writes to a new file and lets reader salvage prior data safely.

8) Corruption handling
If readOne fails or sees invalid message size:

current bad file is renamed to .bad
read pointer jumps to next file
if corruption touches current write file, write side is also advanced
state is marked needSync and reconciled
This is a pragmatic “skip bad segment, continue service” strategy.

9) Empty/Delete/Close
Empty removes queued data files and metadata, resets pointers.
Delete exits queue lifecycle.
Close stops ioLoop and performs final sync.
How this maps to NSQ behavior you observe
“Memory first, disk second”
NSQ tries memory channel first for latency; falls back to diskqueue when memory queue is full or disabled.
See topic.go and channel.go.
At-least-once semantics
Diskqueue durability prevents loss across process restarts for persisted queues.
Delivery state (in-flight/deferred) is channel-managed; requeue/timeout paths are in channel.go.
Shutdown persistence
NSQ closes topics/channels and flushes buffered state before exit in nsqd.go, and channel flush writes remaining in-memory/deferred/in-flight messages to backend in channel.go.
If you want, I can next produce a compact “failure matrix” (crash timing vs what can be lost/duplicated) specifically for current SyncEvery/SyncTimeout settings.

