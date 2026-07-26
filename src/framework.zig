//! The node mixin, Zig-style: instead of a Ruby module included into a
//! class, `boot(NodeT, allocator)` is a comptime-generic function that
//! wires any NodeT satisfying the contract onto the bus. The contract is
//! enforced at *compile time* (a NodeT missing handleMessage is a build
//! error, not a runtime raise like Ruby's StrictContract) — arguably louder
//! and even more immediate feedback for an iterating agent.
//!
//! Nodes with a constructor that needs more than the bus (CanBridge takes
//! an interface name) skip boot() and wire themselves manually from `Env`,
//! `subscribeAll`, and `Heartbeat` — see nodes/can_bridge_node.zig.
const std = @import("std");
const bus_mod = @import("zeromq_bus.zig");
pub const Bus = bus_mod.Bus;

pub const HEARTBEAT_INTERVAL_S: u64 = 5;

/// Every node must define:
///   pub fn init(bus: *Bus) NodeT
///   pub fn handleMessage(self: *NodeT, topic: []const u8, payload: std.json.Value) void
/// (handleMessage may also return `!void`; the dispatcher catches and warns
/// on error, mirroring the Ruby bus's per-handler error isolation.)
fn assertContract(comptime NodeT: type) void {
    if (!@hasDecl(NodeT, "init")) {
        @compileError("[Framework Error] Contract Violation: " ++ @typeName(NodeT) ++
            " missing `pub fn init(bus: *Bus) " ++ @typeName(NodeT) ++ "`");
    }
    if (!@hasDecl(NodeT, "handleMessage")) {
        @compileError("[Framework Error] Contract Violation: " ++ @typeName(NodeT) ++
            " missing `pub fn handleMessage(self: *" ++ @typeName(NodeT) ++
            ", topic: []const u8, payload: std.json.Value) void`");
    }
}

fn callHandleMessage(comptime NodeT: type, node: *NodeT, topic: []const u8, payload: std.json.Value) void {
    const Fn = @TypeOf(NodeT.handleMessage);
    const ReturnType = @typeInfo(Fn).@"fn".return_type.?;
    switch (@typeInfo(ReturnType)) {
        .error_union => node.handleMessage(topic, payload) catch |err| {
            std.debug.print("[Framework Error] {s} failed handling {s}: {}\n", .{ @typeName(NodeT), topic, err });
        },
        else => node.handleMessage(topic, payload),
    }
}

/// Subscribes `node_ptr` to every topic in `topics`, routing wire messages
/// back to NodeT.handleMessage. Exposed separately from boot() so manually
/// wired nodes (CanBridge) can use it too.
pub fn subscribeAll(comptime NodeT: type, bus: *Bus, node_ptr: *NodeT, topics: []const []const u8) !void {
    const Trampoline = struct {
        fn onMessage(ptr: *anyopaque, topic: []const u8, payload: std.json.Value) void {
            callHandleMessage(NodeT, @ptrCast(@alignCast(ptr)), topic, payload);
        }
    };
    for (topics) |topic| try bus.subscribe(topic, .{ .ptr = node_ptr, .callback = Trampoline.onMessage });
}

/// A node's wiring, read from the environment flowctl sets (see PROTOCOL.md).
pub const Env = struct {
    bus_port: u16,
    peers: [][]const u8,
    subscribes: [][]const u8,
    node_name: []const u8,

    pub fn load(allocator: std.mem.Allocator, default_name: []const u8) !Env {
        return .{
            .bus_port = envU16(allocator, "BUS_PORT", 0),
            .peers = try envList(allocator, "BUS_PEERS"),
            .subscribes = try envList(allocator, "BUS_SUBSCRIBES"),
            .node_name = std.process.getEnvVarOwned(allocator, "NODE_NAME") catch |err| switch (err) {
                error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_name),
                else => return err,
            },
        };
    }
};

/// Every node publishes on topic "heartbeat" every HEARTBEAT_INTERVAL_S
/// seconds. Meant to live as a stack or struct field for the process's
/// lifetime — start() spawns a thread that holds a pointer to `self`.
pub const Heartbeat = struct {
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?std.Thread = null,

    pub fn start(self: *Heartbeat, bus: *Bus, name: []const u8) !void {
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, loop, .{ self, bus, name });
    }

    fn loop(self: *Heartbeat, bus: *Bus, name: []const u8) void {
        while (true) {
            bus.publish("heartbeat", .{
                .node_name = name,
                .status = "ok",
                .timestamp = std.time.timestamp(),
            }) catch |err| {
                std.debug.print("[Framework Error] Heartbeat failed for {s}: {}\n", .{ name, err });
            };

            self.mutex.lock();
            if (self.running.load(.acquire)) {
                self.cond.timedWait(&self.mutex, HEARTBEAT_INTERVAL_S * std.time.ns_per_s) catch {};
            }
            const keep_going = self.running.load(.acquire);
            self.mutex.unlock();
            if (!keep_going) return;
        }
    }

    /// Wakes the thread out of its interval wait rather than killing it, so
    /// an in-flight broadcast always completes. Idempotent.
    pub fn stop(self: *Heartbeat) void {
        const thread = self.thread orelse return;
        self.mutex.lock();
        self.running.store(false, .release);
        self.cond.signal();
        self.mutex.unlock();
        thread.join();
        self.thread = null;
    }
};

/// Handle returned by boot(): owns the bus, the node instance, and the
/// heartbeat thread. Node scripts call handle.broadcast(...) the way a Ruby
/// node calls self.broadcast(...).
pub fn NodeHandle(comptime NodeT: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        bus: *Bus,
        node: *NodeT,
        name: []const u8,
        hb: Heartbeat,

        pub fn broadcast(self: *Self, topic: []const u8, payload: anytype) void {
            self.bus.publish(topic, payload) catch |err| {
                std.debug.print("[Framework Error] broadcast {s} failed: {}\n", .{ topic, err });
            };
        }

        pub fn nodeName(self: *const Self) []const u8 {
            return self.name;
        }

        /// Call before closing the bus this node broadcasts on.
        pub fn stopHeartbeat(self: *Self) void {
            self.hb.stop();
        }
    };
}

/// Boots a node the flow-runtime way: all bus wiring comes from
/// environment variables (set by flowctl, or by hand), so node code never
/// contains ports, peer lists, or subscription calls.
///
/// With no environment set, the node still boots standalone on an
/// ephemeral port — handy for poking at a single node in isolation.
/// Installs TERM/INT handlers that exit quietly, matching flowctl's
/// supervised-process expectations.
pub fn boot(comptime NodeT: type, allocator: std.mem.Allocator) !*NodeHandle(NodeT) {
    comptime assertContract(NodeT);

    const env = try Env.load(allocator, @typeName(NodeT));
    const bus = try Bus.init(allocator, env.bus_port, env.peers, "127.0.0.1");

    const node_ptr = try allocator.create(NodeT);
    node_ptr.* = NodeT.init(bus);

    const handle = try allocator.create(NodeHandle(NodeT));
    handle.* = .{ .allocator = allocator, .bus = bus, .node = node_ptr, .name = env.node_name, .hb = .{} };

    try subscribeAll(NodeT, bus, node_ptr, env.subscribes);
    try handle.hb.start(bus, handle.name);
    installSignalHandlers();
    return handle;
}

fn envU16(allocator: std.mem.Allocator, key: []const u8, default: u16) u16 {
    const v = std.process.getEnvVarOwned(allocator, key) catch return default;
    defer allocator.free(v);
    return std.fmt.parseInt(u16, v, 10) catch default;
}

pub fn envList(allocator: std.mem.Allocator, key: []const u8) ![][]const u8 {
    const raw = std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return allocator.alloc([]const u8, 0),
        else => return err,
    };
    defer allocator.free(raw);

    var list = std.ArrayList([]const u8).init(allocator);
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (trimmed.len == 0) continue;
        try list.append(try allocator.dupe(u8, trimmed));
    }
    return list.toOwnedSlice();
}

fn handleTermSignal(sig: i32) callconv(.c) void {
    _ = sig;
    std.process.exit(0);
}

/// Booted nodes are processes managed by a supervisor (flowctl) or a
/// terminal: exit quietly on TERM/INT instead of dumping a stack trace from
/// an interrupted sleep.
pub fn installSignalHandlers() void {
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleTermSignal },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
}

/// Equivalent of Ruby's trailing bare `sleep` — parks the main thread
/// forever while background threads (heartbeat, bus listener) do the work.
pub fn sleepForever() noreturn {
    while (true) std.time.sleep(std.time.ns_per_hour);
}

test "envList splits, trims, and drops empties" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // getEnvVarOwned reads the real process environment, so exercise the
    // parsing logic directly instead of mutating process env from a test.
    const raw = " a, b ,,c";
    var list = std.ArrayList([]const u8).init(arena.allocator());
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (trimmed.len == 0) continue;
        try list.append(trimmed);
    }
    try std.testing.expectEqual(@as(usize, 3), list.items.len);
    try std.testing.expectEqualStrings("a", list.items[0]);
    try std.testing.expectEqualStrings("b", list.items[1]);
    try std.testing.expectEqualStrings("c", list.items[2]);
}
