const std = @import("std");
const zzf = @import("zig_zmq_framework");

/// Runs the library's StateRegistry as a flow node. What it caches is
/// decided entirely by the subscribes list in flow.yml — this file knows
/// nothing about topics. Prints its snapshot every 5 seconds.
pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const handle = try zzf.boot(zzf.StateRegistry, allocator);
    std.debug.print("online\n", .{});

    while (true) {
        std.time.sleep(5 * std.time.ns_per_s);
        std.debug.print("---- Global State Snapshot ----\n", .{});

        var it = handle.node.active_nodes.iterator();
        while (it.next()) |entry| {
            std.debug.print("  active_nodes[{s}] = status={s} timestamp={d}\n", .{
                entry.key_ptr.*, entry.value_ptr.status, entry.value_ptr.timestamp,
            });
        }

        var tit = handle.node.telemetry.iterator();
        while (tit.next()) |entry| {
            const s = std.json.stringifyAlloc(allocator, entry.value_ptr.*, .{}) catch continue;
            defer allocator.free(s);
            std.debug.print("  telemetry[{s}] = {s}\n", .{ entry.key_ptr.*, s });
        }
    }
}
