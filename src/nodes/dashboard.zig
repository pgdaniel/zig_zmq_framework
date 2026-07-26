const std = @import("std");
const zzf = @import("zig_zmq_framework");
const Bus = zzf.Bus;

/// Consumer side of the async state-sync pattern: request the registry's
/// snapshot once on startup, then log whatever comes back.
/// Publishes: request_global_state. Subscribes: global_state_snapshot.
const Dashboard = struct {
    bus: *Bus,

    pub fn init(bus: *Bus) Dashboard {
        return .{ .bus = bus };
    }

    pub fn handleMessage(self: *Dashboard, topic: []const u8, payload: std.json.Value) !void {
        _ = self;
        if (!std.mem.eql(u8, topic, "global_state_snapshot")) return;

        const json_str = try std.json.stringifyAlloc(std.heap.page_allocator, payload, .{});
        defer std.heap.page_allocator.free(json_str);
        std.debug.print("Synced global state: {s}\n", .{json_str});
    }
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const handle = try zzf.boot(Dashboard, allocator);
    std.debug.print("online\n", .{});

    std.time.sleep(std.time.ns_per_s); // let PUB/SUB connections settle before the fire-and-forget request
    handle.broadcast("request_global_state", .{ .requester = handle.nodeName() });
    zzf.framework.sleepForever();
}
