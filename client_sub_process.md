Here is the exact SUB registration flow in NSQ (TCP V2):

Command reaches protocol loop
The client connection is handled by IOLoop in protocol_v2.go:39, which parses incoming commands and dispatches SUB via Exec.

SUB command validation
In protocol_v2.go:618, nsqd rejects SUB unless:

client state is init (not already subscribed)
heartbeats are enabled
parameters include topic and channel
topic/channel names are valid
auth passes if auth is enabled
Topic and channel lookup/creation
SUB calls nsqd.GetTopic(topicName), then topic.GetChannel(channelName).
topic creation/start logic is in nsqd.go
channel creation/lookup is in topic.go
Client registration into the channel
SUB calls AddClient(clientID, client) on the channel in channel.go:455.
AddClient:
rejects if channel is exiting
enforces max-channel-consumers limit
inserts client into channel’s clients map
Ephemeral race handling
SUB has a retry guard for ephemeral topic/channel teardown races.
If channel/topic is ephemeral and exiting right after AddClient, it removes the client and retries once (100ms sleep), else returns E_SUB_FAILED.

Client state transition and message pump wiring
After successful registration, SUB:

sets client state to subscribed
stores client.Channel
sends channel to client.SubEventChan
This wakes messagePump (started earlier by IOLoop) to bind delivery sources to that channel in protocol_v2.go:203.
Delivery does not start until RDY
SUB only registers the client. Actual message flow requires RDY > 0.
messagePump checks readiness via IsReadyForMessages in client_v2.go:417, and RDY updates ready count via client_v2.go:434.

Unregister on disconnect
When the connection exits, IOLoop closes ExitChan and calls RemoveClient on the channel, which removes the client from the channel map in channel.go.

In short: SUB = validate -> get/create topic/channel -> AddClient -> mark subscribed -> bind messagePump; RDY then controls actual consumption.