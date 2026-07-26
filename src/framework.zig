const std = @import("std");
const bus_mod = @import("zeromq_bus.zig");
pub const Bus = bus_mod.Bus;

// std.time functions were removed in Zig 0.16; use the C library instead.
const c_time = @cImport(@cInclude("time.h"));

pub const HEARTBEAT_INTERVAL_S: i64 = 5;

fn nowSecs() i64 {
    return @intCast(c_time.time(null));
}

/// Compile-time contract check. A missing method is a build error, not a runtime panic.
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

/// Subscribes `node_ptr` to every topic in `topics`.  Exposed separately from
/// boot() so that manually-wired nodes can use it without going through boot().
pub fn subscribeAll(comptime NodeT: type, bus: *Bus, node_ptr: *NodeT, topics: []const []const u8) !void {
    const Trampoline = struct {
        fn onMessage(ptr: *anyopaque, topic: []const u8, payload: std.json.Value) void {
            callHandleMessage(NodeT, @ptrCast(@alignCast(ptr)), topic, payload);
        }
    };
    for (topics) |topic| try bus.subscribe(topic, .{ .ptr = node_ptr, .callback = Trampoline.onMessage });
}

/// Wiring read from environment variables set by flowctl (or by hand).
pub const Env = struct {
    bus_port: u16,
    peers: [][]const u8,
    subscribes: [][]const u8,
    node_name: []const u8,

    pub fn load(allocator: std.mem.Allocator, default_name: []const u8) !Env {
        return .{
            .bus_port = envU16("BUS_PORT", 0),
            .peers = try envList(allocator, "BUS_PEERS"),
            .subscribes = try envList(allocator, "BUS_SUBSCRIBES"),
            .node_name = if (getEnvVal("NODE_NAME")) |v|
                try allocator.dupe(u8, v)
            else
                try allocator.dupe(u8, default_name),
        };
    }
};

/// Handle returned by boot().  Owns the bus and the node instance.
/// Call `run()` from main() to start the event loop.
pub fn NodeHandle(comptime NodeT: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        bus: *Bus,
        node: *NodeT,
        name: []const u8,
        last_hb: i64, // seconds since epoch

        /// Publish `payload` serialised as JSON on `topic`.
        pub fn broadcast(self: *Self, topic: []const u8, payload: anytype) void {
            self.bus.publish(topic, payload) catch |err| {
                std.debug.print("[Framework Error] broadcast {s} failed: {}\n", .{ topic, err });
            };
        }

        pub fn nodeName(self: *const Self) []const u8 {
            return self.name;
        }

        /// Drives the event loop forever: sends a heartbeat every
        /// HEARTBEAT_INTERVAL_S seconds and polls the ZMQ bus for
        /// incoming messages in between.  Never returns.
        pub fn run(self: *Self) noreturn {
            while (true) {
                const now = nowSecs();
                if (now - self.last_hb >= HEARTBEAT_INTERVAL_S) {
                    self.bus.publish("heartbeat", .{
                        .node_name = self.name,
                        .status = "ok",
                        .timestamp = now,
                    }) catch |err| {
                        std.debug.print("[Framework Error] Heartbeat failed for {s}: {}\n", .{ self.name, err });
                    };
                    self.last_hb = now;
                }
                self.bus.poll(100);
            }
        }
    };
}

/// Wires `NodeT` onto the bus using environment-supplied config and returns a
/// handle.  Call `handle.run()` afterwards to start the event loop.
pub fn boot(comptime NodeT: type, allocator: std.mem.Allocator) !*NodeHandle(NodeT) {
    comptime assertContract(NodeT);

    const env = try Env.load(allocator, @typeName(NodeT));
    const bus = try Bus.init(allocator, env.bus_port, env.peers, "127.0.0.1");

    const node_ptr = try allocator.create(NodeT);
    node_ptr.* = NodeT.init(bus);

    const handle = try allocator.create(NodeHandle(NodeT));
    handle.* = .{
        .allocator = allocator,
        .bus = bus,
        .node = node_ptr,
        .name = env.node_name,
        .last_hb = nowSecs(),
    };

    try subscribeAll(NodeT, bus, node_ptr, env.subscribes);
    return handle;
}

// --- Environment helpers ---

/// Reads an environment variable as a slice into the process environment block.
/// The returned slice is valid for the lifetime of the process.
fn getEnvVal(key: []const u8) ?[]const u8 {
    var buf: [256:0]u8 = undefined;
    if (key.len >= buf.len) return null;
    @memcpy(buf[0..key.len], key);
    buf[key.len] = 0;
    const raw = std.c.getenv(&buf) orelse return null;
    return std.mem.span(raw);
}

fn envU16(key: []const u8, default: u16) u16 {
    const v = getEnvVal(key) orelse return default;
    return std.fmt.parseInt(u16, v, 10) catch default;
}

pub fn envList(allocator: std.mem.Allocator, key: []const u8) ![][]const u8 {
    const raw = getEnvVal(key) orelse return allocator.alloc([]const u8, 0);
    var list: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (trimmed.len == 0) continue;
        try list.append(allocator, try allocator.dupe(u8, trimmed));
    }
    return list.toOwnedSlice(allocator);
}

test "envList splits, trims, and drops empties" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const raw = " a, b ,,c";
    var list: std.ArrayList([]const u8) = .empty;
    const alloc = arena.allocator();
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (trimmed.len == 0) continue;
        try list.append(alloc, trimmed);
    }
    try std.testing.expectEqual(@as(usize, 3), list.items.len);
    try std.testing.expectEqualStrings("a", list.items[0]);
    try std.testing.expectEqualStrings("b", list.items[1]);
    try std.testing.expectEqualStrings("c", list.items[2]);
}
