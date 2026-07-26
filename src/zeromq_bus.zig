const std = @import("std");
const c = @cImport({
    @cInclude("zmq.h");
});

pub const BusError = error{ZmqError};

/// Type-erased subscriber callback.  `ptr` is the concrete node pointer;
/// `callback` is a comptime-generated trampoline back to the real type.
pub const Subscriber = struct {
    ptr: *anyopaque,
    callback: *const fn (ptr: *anyopaque, topic: []const u8, payload: std.json.Value) void,
};

const Entry = struct { topic: []const u8, sub: Subscriber };

pub const Bus = struct {
    allocator: std.mem.Allocator,
    ctx: *anyopaque,
    pub_sock: *anyopaque,
    sub_sock: *anyopaque,
    subs: std.ArrayList(Entry),
    /// Port the PUB socket is actually bound to (may differ from requested
    /// port when 0 was passed to request an OS-assigned ephemeral port).
    port: u16,

    /// `my_port` may be 0 for an OS-assigned ephemeral port.
    /// `peers` are "host:port" or full "tcp://..." endpoints.
    /// `bind_host` is "127.0.0.1" for loopback-only or "0.0.0.0" for multi-machine.
    pub fn init(
        allocator: std.mem.Allocator,
        my_port: u16,
        peers: []const []const u8,
        bind_host: []const u8,
    ) !*Bus {
        const ctx = c.zmq_ctx_new() orelse return BusError.ZmqError;
        errdefer _ = c.zmq_ctx_term(ctx);

        const pub_sock = c.zmq_socket(ctx, c.ZMQ_PUB) orelse return BusError.ZmqError;
        errdefer _ = c.zmq_close(pub_sock);

        var bind_buf: [128]u8 = undefined;
        const bind_endpoint = if (my_port == 0)
            std.fmt.bufPrintZ(&bind_buf, "tcp://{s}:*", .{bind_host}) catch return BusError.ZmqError
        else
            std.fmt.bufPrintZ(&bind_buf, "tcp://{s}:{d}", .{ bind_host, my_port }) catch return BusError.ZmqError;
        if (c.zmq_bind(pub_sock, bind_endpoint.ptr) != 0) return BusError.ZmqError;

        const sub_sock = c.zmq_socket(ctx, c.ZMQ_SUB) orelse return BusError.ZmqError;
        errdefer _ = c.zmq_close(sub_sock);

        for (peers) |peer| {
            var ep_buf: [256]u8 = undefined;
            const endpoint = if (std.mem.indexOf(u8, peer, "://") != null)
                std.fmt.bufPrintZ(&ep_buf, "{s}", .{peer}) catch return BusError.ZmqError
            else
                std.fmt.bufPrintZ(&ep_buf, "tcp://{s}", .{peer}) catch return BusError.ZmqError;
            _ = c.zmq_connect(sub_sock, endpoint.ptr);
        }

        // Subscribe to everything; topic filtering happens in dispatch().
        if (c.zmq_setsockopt(sub_sock, c.ZMQ_SUBSCRIBE, "", 0) != 0) return BusError.ZmqError;

        const self = try allocator.create(Bus);
        self.* = .{
            .allocator = allocator,
            .ctx = ctx,
            .pub_sock = pub_sock,
            .sub_sock = sub_sock,
            .subs = .empty,
            .port = try readBoundPort(pub_sock),
        };
        return self;
    }

    pub fn subscribe(self: *Bus, topic: []const u8, sub: Subscriber) !void {
        try self.subs.append(self.allocator, .{ .topic = topic, .sub = sub });
    }

    /// Serializes `payload` to JSON, sends it as a two-frame ZMQ multipart
    /// [topic, json], and dispatches it locally so same-process nodes are notified.
    pub fn publish(self: *Bus, topic: []const u8, payload: anytype) !void {
        var json_buf: [4096]u8 = undefined;
        var writer: std.Io.Writer = std.Io.Writer.fixed(&json_buf);
        try std.json.Stringify.value(payload, .{}, &writer);
        const json_bytes = json_buf[0..writer.end];

        if (c.zmq_send(self.pub_sock, topic.ptr, topic.len, c.ZMQ_SNDMORE) == -1)
            return BusError.ZmqError;
        if (c.zmq_send(self.pub_sock, json_bytes.ptr, json_bytes.len, 0) == -1)
            return BusError.ZmqError;

        // Local dispatch: a SUB socket never receives its own PUB, so
        // same-process nodes would otherwise miss each other's messages.
        self.dispatch(topic, json_bytes);
    }

    /// Polls the SUB socket once (blocking up to `timeout_ms`).
    /// If a valid two-frame [topic, json] message arrives, dispatches it.
    /// All errors are silently swallowed — a bad frame never kills the loop.
    pub fn poll(self: *Bus, timeout_ms: c_long) void {
        var item = c.zmq_pollitem_t{
            .socket = self.sub_sock,
            .fd = 0,
            .events = @intCast(c.ZMQ_POLLIN),
            .revents = 0,
        };
        const rc = c.zmq_poll(&item, 1, timeout_ms);
        if (rc <= 0) return;
        if (item.revents & @as(@TypeOf(item.revents), @intCast(c.ZMQ_POLLIN)) == 0) return;

        // Receive topic frame.
        var topic_msg: c.zmq_msg_t = undefined;
        _ = c.zmq_msg_init(&topic_msg);
        if (c.zmq_msg_recv(&topic_msg, self.sub_sock, 0) == -1) {
            _ = c.zmq_msg_close(&topic_msg);
            return;
        }

        var more: c_int = 0;
        var more_size: usize = @sizeOf(c_int);
        _ = c.zmq_getsockopt(self.sub_sock, c.ZMQ_RCVMORE, &more, &more_size);

        const topic_size = c.zmq_msg_size(&topic_msg);
        var topic_buf: [256]u8 = undefined;
        if (topic_size > topic_buf.len) { _ = c.zmq_msg_close(&topic_msg); return; }
        @memcpy(topic_buf[0..topic_size], @as([*]const u8, @ptrCast(c.zmq_msg_data(&topic_msg)))[0..topic_size]);
        _ = c.zmq_msg_close(&topic_msg);

        if (more == 0) return;

        // Receive payload (JSON) frame.
        var payload_msg: c.zmq_msg_t = undefined;
        _ = c.zmq_msg_init(&payload_msg);
        if (c.zmq_msg_recv(&payload_msg, self.sub_sock, 0) == -1) {
            _ = c.zmq_msg_close(&payload_msg);
            return;
        }
        const payload_size = c.zmq_msg_size(&payload_msg);
        var payload_buf: [4096]u8 = undefined;
        if (payload_size > payload_buf.len) { _ = c.zmq_msg_close(&payload_msg); return; }
        @memcpy(payload_buf[0..payload_size], @as([*]const u8, @ptrCast(c.zmq_msg_data(&payload_msg)))[0..payload_size]);
        _ = c.zmq_msg_close(&payload_msg);

        self.dispatch(topic_buf[0..topic_size], payload_buf[0..payload_size]);
    }

    pub fn deinit(self: *Bus) void {
        self.subs.deinit(self.allocator);
        var zero: c_int = 0;
        _ = c.zmq_setsockopt(self.sub_sock, c.ZMQ_LINGER, &zero, @sizeOf(c_int));
        _ = c.zmq_setsockopt(self.pub_sock, c.ZMQ_LINGER, &zero, @sizeOf(c_int));
        _ = c.zmq_close(self.sub_sock);
        _ = c.zmq_close(self.pub_sock);
        _ = c.zmq_ctx_term(self.ctx);
        self.allocator.destroy(self);
    }

    fn dispatch(self: *Bus, topic: []const u8, json: []const u8) void {
        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, json, .{}) catch |err| {
            std.debug.print("[Framework Error] Dropping malformed payload on {s}: {}\n", .{ topic, err });
            return;
        };
        defer parsed.deinit();

        for (self.subs.items) |entry| {
            if (std.mem.eql(u8, entry.topic, topic)) {
                entry.sub.callback(entry.sub.ptr, topic, parsed.value);
            }
        }
    }

    fn readBoundPort(sock: *anyopaque) !u16 {
        var buf: [256]u8 = undefined;
        var len: usize = buf.len;
        if (c.zmq_getsockopt(sock, c.ZMQ_LAST_ENDPOINT, &buf, &len) != 0) return BusError.ZmqError;
        const endpoint = std.mem.sliceTo(buf[0..len], 0);
        const idx = std.mem.lastIndexOfScalar(u8, endpoint, ':') orelse return BusError.ZmqError;
        return std.fmt.parseInt(u16, endpoint[idx + 1 ..], 10) catch BusError.ZmqError;
    }
};

test "bus binds an ephemeral port and reports it back" {
    const bus = try Bus.init(std.testing.allocator, 0, &.{}, "127.0.0.1");
    defer bus.deinit();
    try std.testing.expect(bus.port != 0);
}

test "publish dispatches locally to subscribers on the same bus" {
    const bus = try Bus.init(std.testing.allocator, 0, &.{}, "127.0.0.1");
    defer bus.deinit();

    const Recorder = struct {
        seen: bool = false,
        rpm: i64 = 0,

        fn onMessage(ptr: *anyopaque, _: []const u8, payload: std.json.Value) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen = true;
            self.rpm = payload.object.get("rpm").?.integer;
        }
    };

    var recorder = Recorder{};
    try bus.subscribe("engine_data", .{ .ptr = &recorder, .callback = Recorder.onMessage });
    try bus.publish("engine_data", .{ .rpm = 4200 });

    try std.testing.expect(recorder.seen);
    try std.testing.expectEqual(@as(i64, 4200), recorder.rpm);
}
