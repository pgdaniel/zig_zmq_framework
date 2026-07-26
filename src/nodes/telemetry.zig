const std = @import("std");
const zzf = @import("zig_zmq_framework");
const Bus = zzf.Bus;

const OVER_REV_RPM: i64 = 6000;

/// Watches engine data and commands a throttle cut on over-rev.
/// Publishes: throttle_request. Subscribes: engine_data.
const Telemetry = struct {
    bus: *Bus,

    pub fn init(bus: *Bus) Telemetry {
        return .{ .bus = bus };
    }

    pub fn handleMessage(self: *Telemetry, topic: []const u8, payload: std.json.Value) void {
        if (!std.mem.eql(u8, topic, "engine_data")) return;

        const rpm = payload.object.get("rpm").?.integer;
        std.debug.print("Processing RPM: {d}\n", .{rpm});
        if (rpm <= OVER_REV_RPM) return;

        std.debug.print("OVER-REV DETECTED! Commanding throttle cut...\n", .{});
        self.bus.publish("throttle_request", .{ .position = 50 }) catch |err| {
            std.debug.print("[Framework Error] broadcast failed: {}\n", .{err});
        };
    }
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    _ = try zzf.boot(Telemetry, allocator);
    std.debug.print("online\n", .{});
    zzf.framework.sleepForever();
}
