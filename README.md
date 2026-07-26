# zig_zmq_framework

**A Zig port of [ruby_zmq_framework](https://github.com/pgdaniel/ruby_zmq_framework):
Node-RED without the UI, in a language with no runtime and no GC.**

- **Nodes** are independent OS processes. Each one does one job, lives in one
  file, and knows nothing about any other node — not their ports, not their
  names, not their language.
- **Wires** are pub/sub topics carrying JSON, over ZeroMQ.
- **The graph is data**: [`flow.yml`](flow.yml) is the only artifact that
  knows the topology. `flowctl` reads it, computes the wiring, and runs
  everything.
- **The contract is one page**: [`PROTOCOL.md`](PROTOCOL.md) is everything a
  node in any language needs to join — and it's the *same* contract as the
  Ruby original, so a Ruby node and a Zig node can sit in one `flow.yml`
  together without either knowing the other exists.

The node contract (`init`/`handleMessage`) is enforced at **compile time**:
a node type missing `handleMessage` is a build error, not a runtime
exception — the Zig-native equivalent of the Ruby version's `StrictContract`.

## Quick start

You need Zig 0.14+ and the ZeroMQ library (`libzmq3-dev` on Debian/Ubuntu,
`brew install zeromq` on macOS), then:

```bash
zig build
zig-out/bin/flowctl
```

That runs the demo graph from `flow.yml`: a simulated ECU blasting RPM data,
a telemetry node that commands a throttle cut on over-rev, a web dashboard
on <http://localhost:4567>, a state registry caching heartbeats and
telemetry, and a dashboard consumer syncing the registry's snapshot. Output
is streamed with a `[node_name]` prefix; Ctrl-C stops everything.

`zig-out/bin/flowctl --plan` prints the computed wiring without running
anything. `zig-out/bin/flowctl --graph` prints the node topology as JSON.

## Writing a node

A Zig node is a struct with two functions, booted from the environment:

```zig
const std = @import("std");
const zzf = @import("zig_zmq_framework");
const Bus = zzf.Bus;

const RpmSmoother = struct {
    bus: *Bus,
    window: std.BoundedArray(i64, 5) = .{},

    pub fn init(bus: *Bus) RpmSmoother {
        return .{ .bus = bus };
    }

    pub fn handleMessage(self: *RpmSmoother, topic: []const u8, payload: std.json.Value) void {
        if (!std.mem.eql(u8, topic, "engine_data")) return;
        if (self.window.len == 5) _ = self.window.orderedRemove(0);
        self.window.appendAssumeCapacity(payload.object.get("rpm").?.integer);

        var sum: i64 = 0;
        for (self.window.slice()) |rpm| sum += rpm;
        self.bus.publish("engine_data_smooth", .{ .rpm = @divTrunc(sum, @as(i64, @intCast(self.window.len))) }) catch {};
    }
};

pub fn main() !void {
    const handle = try zzf.boot(RpmSmoother, std.heap.page_allocator);
    _ = handle;
    zzf.framework.sleepForever();
}
```

Note what's absent: no ports, no peers, no subscribe calls. Wiring comes
from environment variables (`BUS_PORT`, `BUS_PEERS`, `BUS_SUBSCRIBES`,
`NODE_NAME` — see `PROTOCOL.md`), which `flowctl` computes from the node's
entry in the manifest:

```yaml
  rpm_smoother:
    cmd: zig-out/bin/rpm_smoother
    subscribes: [engine_data]
    publishes: [engine_data_smooth]
```

(add the executable to `build.zig`'s `exes` list to get it built.)

Run standalone (no environment needed — it binds an ephemeral port) to poke
at a node in isolation: `zig-out/bin/rpm_smoother`.

Every node automatically heartbeats every 5 seconds. A node type that
forgets `handleMessage` or `init` fails `zig build` with a `Contract
Violation` message pointing at the missing method — loud, immediate, and
before the node ever runs.

## Nodes in other languages

The bus is just two-frame ZeroMQ pub/sub — `[topic, json]` — and the whole
contract fits on one page: [`PROTOCOL.md`](PROTOCOL.md), including a
complete minimal Python node and the raw libzmq calls this framework's
`boot()` makes under the hood. Follow it, add a `cmd` entry to `flow.yml`,
and the language never matters again — including the original
[ruby_zmq_framework](https://github.com/pgdaniel/ruby_zmq_framework) and
its [Go](https://github.com/pgdaniel/go_zmq_framework),
[Rust](https://github.com/pgdaniel/rust_zmq_framework),
[Node](https://github.com/pgdaniel/node_zmq_framework), and
[C++](https://github.com/pgdaniel/cpp_zmq_framework) ports, which all
speak the exact same wire format.
[flow_viewer](https://github.com/pgdaniel/flow_viewer) can view and edit
any of their `flow.yml` files.

## What's in the box

| piece | file | job |
|-------|------|-----|
| `Bus` | `src/zeromq_bus.zig` | hardened transport: poison-message-proof listener, per-handler error isolation, local dispatch, clean `close` |
| `boot()` / node contract | `src/framework.zig` | comptime contract enforcement, auto-heartbeat, `NodeHandle.broadcast`, `Env`, `Heartbeat` |
| `Flow` | `src/flow.zig` | parses `flow.yml`, computes each node's env wiring and the `--graph` topology |
| `flowctl` | `src/flowctl.zig` | assigns ports, spawns nodes, prefixes output, tears down on Ctrl-C |
| `StateRegistry` | `src/state_registry.zig` | passive cluster-state cache; replays snapshots on request |
| `CanBridge` | `src/can_bridge.zig` | real SocketCAN frames → `can_frame` topic (classic CAN, via raw syscalls, no extra dependency) |
| demo nodes | `src/nodes/*.zig` | one blackbox process per file |

Delivery is fire-and-forget (latest-value-wins; slow consumers drop old
messages), handlers on one bus never run concurrently (a recursive mutex
serializes them the same way the Ruby version's `Monitor` does), and a bad
message or an erroring handler can never kill a node's listener thread.

> **Note:** ZeroMQ is reached through a direct `@cImport(zmq.h)` binding —
> no vendored wrapper. The wire format is deliberately plain two-frame
> PUB/SUB, so swapping transports stays a contained change behind `Bus`'s
> three-method interface (`publish`/`subscribe`/`close`).

## CAN hardware

Uncomment the `can_bridge` node in `flow.yml` (set `CAN_IFACE`, e.g.
`vcan0`) to relay real SocketCAN frames onto the bus as `can_frame`
messages. Needs an actual or virtual CAN interface; fails fast if it
doesn't exist. `CanBridge` doesn't fit `boot()`'s `init(bus)` signature
(it also needs an interface name), so `src/nodes/can_bridge_node.zig`
wires itself up by hand from the same `Env`/`Heartbeat` pieces `boot()`
uses internally — see that file for the pattern if your own node needs
extra constructor arguments.

## Tests

```bash
zig build test
```

Unit tests live as `test { ... }` blocks alongside the code they cover
(`src/zeromq_bus.zig`, `src/flow.zig`, `src/state_registry.zig`,
`src/can_bridge.zig`, `src/framework.zig`), mirroring the Ruby version's
Minitest suite: bus dispatch, flow wiring/graph computation, and
StateRegistry's heartbeat/telemetry caching behavior.
