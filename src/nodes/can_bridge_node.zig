const std = @import("std");
const zzf = @import("zig_zmq_framework");
const fw = zzf.framework;
const Bus = zzf.Bus;
const CanBridge = zzf.CanBridge;

/// Relays raw SocketCAN frames onto the bus. Publishes: can_frame.
/// Needs a real or virtual CAN interface (set CAN_IFACE, default can0);
/// fails fast if it doesn't exist.
///
/// CanBridge.open() needs an interface name in addition to the bus, so
/// unlike the other nodes this one wires itself up by hand instead of
/// going through framework.boot() (see framework.zig's doc comment).
pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const env = try fw.Env.load(allocator, "CanBridge");
    const bus = try Bus.init(allocator, env.bus_port, env.peers, "127.0.0.1");

    const interface = std.process.getEnvVarOwned(allocator, "CAN_IFACE") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, "can0"),
        else => return err,
    };

    var bridge = try CanBridge.open(bus, interface, "can_frame");
    try fw.subscribeAll(CanBridge, bus, &bridge, env.subscribes);

    var hb = fw.Heartbeat{};
    try hb.start(bus, env.node_name);
    fw.installSignalHandlers();

    std.debug.print("online (reading {s}, broadcasting :can_frame)\n", .{interface});
    fw.sleepForever();
}
