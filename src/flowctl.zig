//! Runs a flow manifest: assigns each node a free port, computes the peer
//! wiring, spawns every node with that wiring in its environment, and
//! streams their output with a [name] prefix. Ctrl-C stops everything.
//!
//!   flowctl              # runs ./flow.yml
//!   flowctl other.yml
//!   flowctl --plan       # print computed wiring, run nothing
//!   flowctl --graph      # print the topology as JSON, run nothing
const std = @import("std");
const posix = std.posix;
const zzf = @import("zig_zmq_framework");
const Flow = zzf.flow.Flow;

const MAX_CHILDREN = 64;
var g_child_pids: [MAX_CHILDREN]posix.pid_t = undefined;
var g_child_count: usize = 0;

const Child = struct { pid: posix.pid_t, name: []const u8 };

pub fn main() !void {
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const args = try std.process.argsAlloc(a);

    var plan_only = false;
    var graph_only = false;
    var manifest: []const u8 = "flow.yml";
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--plan")) {
            plan_only = true;
        } else if (std.mem.eql(u8, arg, "--graph")) {
            graph_only = true;
        } else if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            try std.io.getStdOut().writer().print("Usage: flowctl [--plan | --graph] [flow.yml]\n", .{});
            return;
        } else {
            manifest = arg;
        }
    }

    const cwd = try std.process.getCwdAlloc(a);
    const manifest_path = try std.fs.path.resolve(a, &.{ cwd, manifest });
    const root = std.fs.path.dirname(manifest_path) orelse "/";

    var diag = zzf.flow.Diagnostics{};
    var flow = Flow.loadFile(a, manifest_path, &diag) catch |err| {
        std.debug.print("{s}\n", .{diag.message});
        return err;
    };
    defer flow.deinit();

    if (graph_only) {
        const g = try flow.graph(a);
        const stdout = std.io.getStdOut().writer();
        try std.json.stringify(g, .{ .whitespace = .indent_2 }, stdout);
        try stdout.print("\n", .{});
        return;
    }

    var ports = std.StringHashMap(u16).init(a);
    for (flow.nodes) |node| try ports.put(node.name, try freePort());

    const wiring = try flow.wiring(a, ports);

    if (plan_only) {
        const stdout = std.io.getStdOut().writer();
        for (wiring) |entry| {
            try stdout.print("{s}\n", .{entry.node_name});
            for (entry.env) |pair| try stdout.print("  {s}={s}\n", .{ pair.key, pair.value });
        }
        return;
    }

    var wiring_by_name = std.StringHashMap([]const zzf.flow.EnvPair).init(a);
    for (wiring) |entry| try wiring_by_name.put(entry.node_name, entry.env);

    var print_lock = std.Thread.Mutex{};
    var children = std.ArrayList(Child).init(a);

    for (flow.nodes) |node| {
        var env_map = try std.process.getEnvMap(a);
        for (wiring_by_name.get(node.name).?) |pair| try env_map.put(pair.key, pair.value);

        var child = std.process.Child.init(&.{ "sh", "-c", node.cmd }, a);
        child.cwd = root;
        child.env_map = &env_map;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        try child.spawn();

        g_child_pids[g_child_count] = child.id;
        g_child_count += 1;
        try children.append(.{ .pid = child.id, .name = node.name });

        const stdout_file = child.stdout.?;
        const stderr_file = child.stderr.?;
        _ = try std.Thread.spawn(.{}, pumpOutput, .{ stdout_file, node.name, &print_lock });
        _ = try std.Thread.spawn(.{}, pumpOutput, .{ stderr_file, node.name, &print_lock });
    }

    installInterruptHandler();

    var names = std.ArrayList(u8).init(a);
    for (children.items, 0..) |c, i| {
        if (i != 0) try names.appendSlice(", ");
        try names.appendSlice(c.name);
    }
    std.debug.print("[flowctl] started {d} nodes: {s}\n", .{ children.items.len, names.items });

    var remaining = children.items.len;
    while (remaining > 0) {
        const result = posix.waitpid(-1, 0);
        const name = nameForPid(children.items, result.pid) orelse "?";
        if (posix.W.IFEXITED(result.status)) {
            std.debug.print("[flowctl] {s} exited with status {d}\n", .{ name, posix.W.EXITSTATUS(result.status) });
        } else {
            std.debug.print("[flowctl] {s} exited (signal {d})\n", .{ name, posix.W.TERMSIG(result.status) });
        }
        remaining -= 1;
    }
    std.debug.print("[flowctl] all nodes exited\n", .{});
}

fn nameForPid(children: []const Child, pid: posix.pid_t) ?[]const u8 {
    for (children) |c| {
        if (c.pid == pid) return c.name;
    }
    return null;
}

fn freePort() !u16 {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();
    return server.listen_address.getPort();
}

fn pumpOutput(file: std.fs.File, name: []const u8, print_lock: *std.Thread.Mutex) void {
    defer file.close();
    var buf_reader = std.io.bufferedReader(file.reader());
    const reader = buf_reader.reader();

    var line = std.ArrayList(u8).init(std.heap.page_allocator);
    defer line.deinit();

    while (true) {
        line.clearRetainingCapacity();
        reader.streamUntilDelimiter(line.writer(), '\n', null) catch |err| {
            if (line.items.len > 0) {
                print_lock.lock();
                std.debug.print("[{s}] {s}\n", .{ name, line.items });
                print_lock.unlock();
            }
            if (err != error.EndOfStream) {}
            return;
        };
        print_lock.lock();
        std.debug.print("[{s}] {s}\n", .{ name, line.items });
        print_lock.unlock();
    }
}

/// Ctrl-C stops everything: send TERM to every spawned node and exit.
/// Deliberately doesn't wait for them to reap here (an interrupt handler is
/// no place to synchronize with reader threads still touching shared
/// state) — sending TERM is enough, since every node installs its own
/// TERM handler and exits promptly (see framework.installSignalHandlers).
fn handleInterrupt(sig: i32) callconv(.c) void {
    _ = sig;
    const msg = "\n[flowctl] shutting down\n";
    _ = posix.write(2, msg) catch {};
    for (g_child_pids[0..g_child_count]) |pid| {
        posix.kill(pid, posix.SIG.TERM) catch {};
    }
    std.process.exit(0);
}

fn installInterruptHandler() void {
    const act = posix.Sigaction{
        .handler = .{ .handler = handleInterrupt },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &act, null);
    posix.sigaction(posix.SIG.TERM, &act, null);
}
