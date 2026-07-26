# Bus Protocol

This one page is the entire contract. A node written in any language that
follows it is a full citizen of the flow — no framework code required.
It is deliberately small enough to paste into an LLM prompt as the
complete context for writing a new node.

This is the same protocol as
[ruby_zmq_framework](https://github.com/pgdaniel/ruby_zmq_framework): a Ruby
node and a Zig node from this repo are interchangeable entries in one
`flow.yml`, because the wire format never mentions either language.

## Transport

ZeroMQ PUB/SUB over TCP. Every node owns exactly one PUB socket (bound to
its own port, where it publishes everything) and one SUB socket
(connected to each peer's PUB port, subscribed to `""` — all topics —
with filtering done after receipt).

## Wire format

Every message is a two-frame ZeroMQ multipart message:

| frame | content                                    |
|-------|--------------------------------------------|
| 0     | topic — UTF-8 string, `lower_snake_case`   |
| 1     | payload — a JSON **object** (`{...}`), UTF-8 |

Anything else (single frames, non-JSON payloads) is dropped by
well-behaved receivers with a warning. Never crash on a bad message.

## Process environment (set by the launcher)

A node learns its wiring from four environment variables and must not
hardcode any topology:

| variable         | meaning                                            | example                         |
|------------------|----------------------------------------------------|---------------------------------|
| `BUS_PORT`       | port to bind the PUB socket on (`0`/unset = any)   | `5555`                          |
| `BUS_PEERS`      | comma-separated `host:port` list to connect SUB to | `127.0.0.1:5556,127.0.0.1:5557` |
| `BUS_SUBSCRIBES` | comma-separated topics this node should act on     | `engine_data,heartbeat`         |
| `NODE_NAME`      | this node's identity, used in heartbeats           | `ecu`                           |

Unset variables mean: bind any free port, no peers, no subscriptions —
the node must still start (standalone mode).

## Heartbeat

Every node publishes on topic `heartbeat` every **5 seconds**:

```json
{ "node_name": "<NODE_NAME>", "status": "ok", "timestamp": 1784796766 }
```

`timestamp` is Unix seconds. A node that stops heartbeating is presumed
dead; there is no other liveness mechanism.

## Conventions

- Payloads are always JSON objects, never bare arrays/scalars.
- Delivery is fire-and-forget: no acks, no replay. A slow consumer loses
  old messages (ZeroMQ drops at the high-water mark). Design for
  latest-value-wins.
- PUB/SUB connections take ~a few hundred ms to establish ("slow
  joiner"): wait ~1s after startup before your first meaningful publish,
  or design so early messages are harmless to lose.
- Request/response is done with a topic pair by convention, e.g. publish
  `request_global_state` → someone publishes `global_state_snapshot`.

## Minimal node, any language (Python shown)

```python
import json, os, time, threading, zmq

ctx = zmq.Context()
pub = ctx.socket(zmq.PUB); pub.bind(f"tcp://127.0.0.1:{os.environ.get('BUS_PORT', '*')}")
sub = ctx.socket(zmq.SUB); sub.setsockopt_string(zmq.SUBSCRIBE, "")
for peer in filter(None, os.environ.get("BUS_PEERS", "").split(",")):
    sub.connect(f"tcp://{peer}")

name = os.environ.get("NODE_NAME", "python-node")
topics = set(filter(None, os.environ.get("BUS_SUBSCRIBES", "").split(",")))

def heartbeat():
    while True:
        pub.send_multipart([b"heartbeat", json.dumps(
            {"node_name": name, "status": "ok", "timestamp": int(time.time())}).encode()])
        time.sleep(5)
threading.Thread(target=heartbeat, daemon=True).start()

while True:
    topic, payload = sub.recv_multipart()
    if topic.decode() in topics:
        handle(topic.decode(), json.loads(payload))  # your logic here
```

## The same node, raw Zig (no framework)

This is what `src/framework.zig`'s `boot()` does under the hood — shown
bare so the protocol stays checkable against real code, not just prose.

```zig
const std = @import("std");
const c = @cImport({ @cInclude("zmq.h"); });

pub fn main() !void {
    const ctx = c.zmq_ctx_new().?;
    const pub_sock = c.zmq_socket(ctx, c.ZMQ_PUB).?;
    _ = c.zmq_bind(pub_sock, "tcp://127.0.0.1:*");

    const sub_sock = c.zmq_socket(ctx, c.ZMQ_SUB).?;
    // connect sub_sock to each host:port in BUS_PEERS ...
    _ = c.zmq_setsockopt(sub_sock, c.ZMQ_SUBSCRIBE, "", 0);

    // heartbeat thread: every 5s, zmq_send "heartbeat" then the JSON payload
    // with ZMQ_SNDMORE on the first frame, 0 on the second.

    while (true) {
        // zmq_msg_recv the topic frame, check ZMQ_RCVMORE, zmq_msg_recv the
        // payload frame, parse JSON, dispatch if the topic is in BUS_SUBSCRIBES.
    }
}
```

Add the node to `flow.yml` with its `cmd`, `publishes`, and `subscribes`,
and `flowctl` wires it in.
