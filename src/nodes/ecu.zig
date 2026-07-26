const std = @import("std");
const zzf = @import("zig_zmq_framework");
const Bus = zzf.Bus;

/// Simulated engine unit. Publishes: engine_data. Subscribes: throttle_request.
const Ecu = struct {
    bus: *Bus,

    pub fn init(bus: *Bus) Ecu {
        return .{ .bus = bus };
    }

    pub fn handleMessage(self: *Ecu, topic: []const u8, payload: std.json.Value) void {
        _ = self;
        if (std.mem.eql(u8, topic, "throttle_request")) {
            std.debug.print("Received throttle command: {d}%\n", .{payload.object.get("position").?.integer});
        }
    }
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const handle = try zzf.boot(Ecu, allocator);
    std.debug.print("online\n", .{});

    std.time.sleep(std.time.ns_per_s); // let PUB/SUB connections settle before the first broadcast
    var rng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    while (true) {
        const rpm = rng.random().intRangeAtMost(i64, 2000, 7000);
        std.debug.print("Broadcasting RPM: {d}\n", .{rpm});
        handle.broadcast("engine_data", .{ .rpm = rpm });
        std.time.sleep(std.time.ns_per_s);
    }
}
