//! A passive, in-memory cache of cluster-wide state. It listens to
//! heartbeats and telemetry broadcast by other nodes and, on request,
//! replays its current snapshot back onto the bus. It never makes a
//! blocking call and never crashes when a peer goes quiet — a silent node
//! simply stops getting its active_nodes timestamp updated.
const std = @import("std");
const bus_mod = @import("zeromq_bus.zig");
const Bus = bus_mod.Bus;

pub const NodeStatus = struct { status: []const u8, timestamp: i64 };

pub const StateRegistry = struct {
    bus: *Bus,
    allocator: std.mem.Allocator,
    active_nodes: std.StringHashMap(NodeStatus),
    telemetry: std.StringHashMap(std.json.Value),

    pub fn init(bus: *Bus) StateRegistry {
        return initWithAllocator(bus, std.heap.page_allocator);
    }

    pub fn initWithAllocator(bus: *Bus, allocator: std.mem.Allocator) StateRegistry {
        return .{
            .bus = bus,
            .allocator = allocator,
            .active_nodes = std.StringHashMap(NodeStatus).init(allocator),
            .telemetry = std.StringHashMap(std.json.Value).init(allocator),
        };
    }

    pub fn deinit(self: *StateRegistry) void {
        var it = self.active_nodes.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.status);
        }
        self.active_nodes.deinit();

        var tit = self.telemetry.iterator();
        while (tit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeValue(self.allocator, entry.value_ptr.*);
        }
        self.telemetry.deinit();
    }

    pub fn handleMessage(self: *StateRegistry, topic: []const u8, payload: std.json.Value) !void {
        if (std.mem.eql(u8, topic, "heartbeat")) {
            try self.handleHeartbeat(payload);
        } else if (std.mem.eql(u8, topic, "request_global_state")) {
            try self.replaySnapshot();
        } else {
            try self.handleTelemetry(topic, payload);
        }
    }

    fn handleHeartbeat(self: *StateRegistry, payload: std.json.Value) !void {
        const node_name = payload.object.get("node_name").?.string;
        const status = payload.object.get("status").?.string;
        const timestamp = payload.object.get("timestamp").?.integer;

        const gop = try self.active_nodes.getOrPut(node_name);
        if (gop.found_existing) {
            self.allocator.free(gop.value_ptr.status);
        } else {
            gop.key_ptr.* = try self.allocator.dupe(u8, node_name);
        }
        gop.value_ptr.* = .{ .status = try self.allocator.dupe(u8, status), .timestamp = timestamp };
    }

    fn handleTelemetry(self: *StateRegistry, topic: []const u8, payload: std.json.Value) !void {
        const cloned = try cloneValue(self.allocator, payload);

        const gop = try self.telemetry.getOrPut(topic);
        if (gop.found_existing) {
            freeValue(self.allocator, gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.allocator.dupe(u8, topic);
        }
        gop.value_ptr.* = cloned;
    }

    fn replaySnapshot(self: *StateRegistry) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const snapshot = try self.snapshotValue(arena.allocator());
        try self.bus.publish("global_state_snapshot", snapshot);
    }

    /// Builds a std.json.Value tree that aliases the strings/values already
    /// owned by active_nodes/telemetry — safe because it's only read during
    /// this call (bus.publish serializes it to a string immediately).
    fn snapshotValue(self: *StateRegistry, allocator: std.mem.Allocator) !std.json.Value {
        var active = std.json.ObjectMap.init(allocator);
        var it = self.active_nodes.iterator();
        while (it.next()) |entry| {
            var o = std.json.ObjectMap.init(allocator);
            try o.put("status", .{ .string = entry.value_ptr.status });
            try o.put("timestamp", .{ .integer = entry.value_ptr.timestamp });
            try active.put(entry.key_ptr.*, .{ .object = o });
        }

        var telem = std.json.ObjectMap.init(allocator);
        var tit = self.telemetry.iterator();
        while (tit.next()) |entry| try telem.put(entry.key_ptr.*, entry.value_ptr.*);

        var root = std.json.ObjectMap.init(allocator);
        try root.put("active_nodes", .{ .object = active });
        try root.put("telemetry", .{ .object = telem });
        return .{ .object = root };
    }
};

fn cloneValue(allocator: std.mem.Allocator, v: std.json.Value) std.mem.Allocator.Error!std.json.Value {
    return switch (v) {
        .null, .bool, .integer, .float => v,
        .number_string => |s| .{ .number_string = try allocator.dupe(u8, s) },
        .string => |s| .{ .string = try allocator.dupe(u8, s) },
        .array => |arr| blk: {
            var out = std.json.Array.init(allocator);
            try out.ensureTotalCapacity(arr.items.len);
            for (arr.items) |item| out.appendAssumeCapacity(try cloneValue(allocator, item));
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out = std.json.ObjectMap.init(allocator);
            var it = obj.iterator();
            while (it.next()) |entry| {
                try out.put(try allocator.dupe(u8, entry.key_ptr.*), try cloneValue(allocator, entry.value_ptr.*));
            }
            break :blk .{ .object = out };
        },
    };
}

fn freeValue(allocator: std.mem.Allocator, v: std.json.Value) void {
    switch (v) {
        .string, .number_string => |s| allocator.free(s),
        .array => |arr| {
            for (arr.items) |item| freeValue(allocator, item);
            var a = arr;
            a.deinit();
        },
        .object => |obj| {
            var o = obj;
            var it = o.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                freeValue(allocator, entry.value_ptr.*);
            }
            o.deinit();
        },
        else => {},
    }
}

test "starts with an empty store, heartbeat updates active_nodes, arbitrary topics cache as telemetry" {
    const bus = try Bus.init(std.testing.allocator, 0, &.{}, "127.0.0.1");
    defer bus.close();
    defer std.testing.allocator.destroy(bus);

    var registry = StateRegistry.initWithAllocator(bus, std.testing.allocator);
    defer registry.deinit();

    try std.testing.expectEqual(@as(usize, 0), registry.active_nodes.count());

    var hb = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"node_name\":\"Foo\",\"status\":\"ok\",\"timestamp\":123}", .{});
    defer hb.deinit();
    try registry.handleMessage("heartbeat", hb.value);

    try std.testing.expectEqualStrings("ok", registry.active_nodes.get("Foo").?.status);
    try std.testing.expectEqual(@as(i64, 123), registry.active_nodes.get("Foo").?.timestamp);

    var engine = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"rpm\":4200}", .{});
    defer engine.deinit();
    try registry.handleMessage("engine_data", engine.value);
    try std.testing.expectEqual(@as(i64, 4200), registry.telemetry.get("engine_data").?.object.get("rpm").?.integer);
}

test "request_global_state broadcasts the current store locally" {
    const bus = try Bus.init(std.testing.allocator, 0, &.{}, "127.0.0.1");
    defer bus.close();
    defer std.testing.allocator.destroy(bus);

    var registry = StateRegistry.initWithAllocator(bus, std.testing.allocator);
    defer registry.deinit();

    var hb = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"node_name\":\"Foo\",\"status\":\"ok\",\"timestamp\":123}", .{});
    defer hb.deinit();
    try registry.handleMessage("heartbeat", hb.value);

    const Recorder = struct {
        rpm_seen: bool = false,

        fn onMessage(ptr: *anyopaque, topic: []const u8, payload: std.json.Value) void {
            _ = topic;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.rpm_seen = payload.object.get("active_nodes").?.object.contains("Foo");
        }
    };
    var recorder = Recorder{};
    try bus.subscribe("global_state_snapshot", .{ .ptr = &recorder, .callback = Recorder.onMessage });

    var req = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"requester\":\"Dashboard\"}", .{});
    defer req.deinit();
    try registry.handleMessage("request_global_state", req.value);

    try std.testing.expect(recorder.rpm_seen);
}
