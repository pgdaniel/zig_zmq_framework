const std = @import("std");
const zzf = @import("zig_zmq_framework");
const Bus = zzf.Bus;

/// Shared between the bus dispatch thread and HTTP connection threads.
/// Demo-only shortcut: a Mutex around two scalars is cheap insurance, but
/// don't take this as a pattern for anything bigger.
var g_lock: std.Thread.Mutex = .{};
var g_rpm: i64 = 0;
var g_status: []const u8 = "Waiting for data...";

/// HTTP bridge onto the bus: shows live telemetry, sends commands back.
/// Publishes: throttle_request. Subscribes: engine_data.
const WebBridge = struct {
    bus: *Bus,

    pub fn init(bus: *Bus) WebBridge {
        return .{ .bus = bus };
    }

    pub fn handleMessage(self: *WebBridge, topic: []const u8, payload: std.json.Value) void {
        _ = self;
        if (!std.mem.eql(u8, topic, "engine_data")) return;

        g_lock.lock();
        defer g_lock.unlock();
        g_rpm = payload.object.get("rpm").?.integer;
        g_status = "Live";
    }
};

const PAGE_TEMPLATE =
    \\<!DOCTYPE html>
    \\<html>
    \\<head>
    \\  <title>ZMQ Telemetry Dashboard</title>
    \\  <style>
    \\    body {{ font-family: system-ui, sans-serif; background: #111; color: #eee; padding: 2rem; }}
    \\    .card {{ background: #222; padding: 1.5rem; border-radius: 8px; max-width: 400px; margin-bottom: 1rem;}}
    \\    h1 {{ margin-top: 0; color: #4ade80; }}
    \\    button {{ background: #ef4444; color: white; border: none; padding: 10px 15px; border-radius: 5px; cursor: pointer; }}
    \\    button:hover {{ background: #dc2626; }}
    \\  </style>
    \\  <meta http-equiv="refresh" content="1">
    \\</head>
    \\<body>
    \\  <div class="card">
    \\    <h1>Telemetry Dashboard</h1>
    \\    <p><strong>Status:</strong> {s}</p>
    \\    <p><strong>RPM:</strong> <span style="font-size: 1.5em; font-weight: bold;">{d}</span></p>
    \\  </div>
    \\  <div class="card">
    \\    <h2>Overrides</h2>
    \\    <form action="/command" method="POST">
    \\      <input type="hidden" name="throttle" value="0">
    \\      <button type="submit">Send Engine Kill (0% Throttle)</button>
    \\    </form>
    \\  </div>
    \\</body>
    \\</html>
    \\
;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const handle = try zzf.boot(WebBridge, allocator);
    std.debug.print("online\n", .{});

    const port = envPort(allocator, "WEB_PORT", 4567);
    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    while (true) {
        const conn = server.accept() catch continue;
        _ = std.Thread.spawn(.{}, handleConnection, .{ conn, handle }) catch {
            conn.stream.close();
            continue;
        };
    }
}

fn handleConnection(conn: std.net.Server.Connection, handle: *zzf.framework.NodeHandle(WebBridge)) void {
    defer conn.stream.close();
    var read_buffer: [8192]u8 = undefined;
    var http_server = std.http.Server.init(conn, &read_buffer);

    while (http_server.state == .ready) {
        var request = http_server.receiveHead() catch return;
        handleRequest(&request, handle) catch return;
    }
}

fn handleRequest(request: *std.http.Server.Request, handle: *zzf.framework.NodeHandle(WebBridge)) !void {
    if (request.head.method == .GET and std.mem.eql(u8, request.head.target, "/")) {
        g_lock.lock();
        const rpm = g_rpm;
        const status = g_status;
        g_lock.unlock();

        var buf: [4096]u8 = undefined;
        const body = try std.fmt.bufPrint(&buf, PAGE_TEMPLATE, .{ status, rpm });
        try request.respond(body, .{
            .extra_headers = &.{.{ .name = "content-type", .value = "text/html" }},
        });
        return;
    }

    if (request.head.method == .POST and std.mem.eql(u8, request.head.target, "/command")) {
        const len = @min(request.head.content_length orelse 0, 256);
        var body_buf: [256]u8 = undefined;
        const reader = try request.reader();
        const read_len = try reader.readAll(body_buf[0..len]);

        const throttle_pos = parseThrottle(body_buf[0..read_len]);
        handle.broadcast("throttle_request", .{ .position = throttle_pos });
        std.debug.print("Broadcasted throttle command: {d}%\n", .{throttle_pos});

        try request.respond("", .{
            .status = .found,
            .extra_headers = &.{.{ .name = "location", .value = "/" }},
        });
        return;
    }

    try request.respond("Not Found", .{ .status = .not_found });
}

fn parseThrottle(body: []const u8) i64 {
    const key = "throttle=";
    const idx = std.mem.indexOf(u8, body, key) orelse return 0;
    const rest = body[idx + key.len ..];
    var end: usize = 0;
    while (end < rest.len and (std.ascii.isDigit(rest[end]) or rest[end] == '-')) : (end += 1) {}
    return std.fmt.parseInt(i64, rest[0..end], 10) catch 0;
}

fn envPort(allocator: std.mem.Allocator, key: []const u8, default: u16) u16 {
    const v = std.process.getEnvVarOwned(allocator, key) catch return default;
    defer allocator.free(v);
    return std.fmt.parseInt(u16, v, 10) catch default;
}
