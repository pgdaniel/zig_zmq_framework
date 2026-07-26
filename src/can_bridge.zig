//! Reads raw frames off a real SocketCAN interface (can0, vcan0, ...) and
//! rebroadcasts each one onto the ZeroMQ bus. Pure producer: it has no
//! interest in bus traffic, so handleMessage is a no-op that only exists to
//! satisfy the node contract.
//!
//! Talks to the kernel directly via raw sockets + a couple of ioctl/struct
//! calls (see linux/can.h, linux/sockios.h), same approach as the Ruby
//! version, since Zig's std lib doesn't ship SocketCAN support either.
//!
//! Classic CAN only: reads assume the 16-byte struct can_frame. That is
//! safe even on an FD-enabled interface — the kernel only delivers 72-byte
//! canfd_frames to sockets that opt in via CAN_RAW_FD_FRAMES, which this
//! one never does — but it means FD traffic is invisible here.
const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const bus_mod = @import("zeromq_bus.zig");
const Bus = bus_mod.Bus;

const AF_CAN: u32 = 29;
const CAN_RAW: u32 = 1;
const FRAME_SIZE: usize = 16; // sizeof(struct can_frame): 4(id) + 1(len) + 3(pad) + 8(data)
const CAN_EFF_FLAG: u32 = 0x80000000;
const CAN_EFF_MASK: u32 = 0x1FFFFFFF;
const CAN_SFF_MASK: u32 = 0x7FF;

pub const Frame = struct {
    id: u32,
    extended: bool,
    dlc: u8,
    data: [8]u8,

    pub fn dataSlice(self: *const Frame) []const u8 {
        return self.data[0..self.dlc];
    }
};

pub fn parseFrame(raw: []const u8) Frame {
    const id_raw = std.mem.readInt(u32, raw[0..4], .little);
    const len = @min(raw[4], 8);
    const extended = (id_raw & CAN_EFF_FLAG) != 0;
    const can_id = id_raw & (if (extended) CAN_EFF_MASK else CAN_SFF_MASK);

    var data: [8]u8 = [_]u8{0} ** 8;
    @memcpy(data[0..len], raw[8..][0..len]);
    return .{ .id = can_id, .extended = extended, .dlc = len, .data = data };
}

pub const CanBridge = struct {
    bus: *Bus,
    topic: []const u8,
    socket: posix.socket_t,
    running: std.atomic.Value(bool),
    reader: ?std.Thread,

    /// Opens the CAN socket and starts the reader thread. `topic` must
    /// outlive the CanBridge (a string literal or flow-derived constant is
    /// fine — nodes are long-lived processes).
    pub fn open(bus: *Bus, interface: []const u8, topic: []const u8) !CanBridge {
        const sock = try posix.socket(AF_CAN, posix.SOCK.RAW, CAN_RAW);
        errdefer posix.close(sock);

        const ifindex = try interfaceIndex(sock, interface);

        // struct sockaddr_can { sa_family_t can_family; int can_ifindex; ... 8 more bytes ... };
        var addr_buf: [16]u8 align(@alignOf(posix.sockaddr)) = [_]u8{0} ** 16;
        std.mem.writeInt(u16, addr_buf[0..2], @intCast(AF_CAN), native_endian);
        std.mem.writeInt(i32, addr_buf[4..8], ifindex, native_endian);
        const sockaddr: *const posix.sockaddr = @ptrCast(&addr_buf);
        try posix.bind(sock, sockaddr, addr_buf.len);

        var self = CanBridge{
            .bus = bus,
            .topic = topic,
            .socket = sock,
            .running = std.atomic.Value(bool).init(true),
            .reader = null,
        };
        self.reader = try std.Thread.spawn(.{}, readerLoop, .{&self});
        return self;
    }

    pub fn handleMessage(self: *CanBridge, topic: []const u8, payload: std.json.Value) void {
        _ = self;
        _ = topic;
        _ = payload;
    }

    /// Stops the reader thread and closes the CAN socket. Closing the
    /// socket from here is what interrupts the reader's blocking read.
    pub fn close(self: *CanBridge) void {
        if (!self.running.load(.acquire)) return;
        self.running.store(false, .release);
        posix.close(self.socket);
        if (self.reader) |t| t.join();
    }

    fn readerLoop(self: *CanBridge) void {
        var buf: [FRAME_SIZE]u8 = undefined;
        while (self.running.load(.acquire)) {
            const n = posix.read(self.socket, &buf) catch |err| {
                if (!self.running.load(.acquire)) return; // close() interrupted the blocking read
                std.debug.print("[Framework Error] CanBridge read failed: {}\n", .{err});
                std.time.sleep(std.time.ns_per_s);
                continue;
            };
            if (n != FRAME_SIZE) continue;

            const frame = parseFrame(&buf);
            self.bus.publish(self.topic, .{
                .id = frame.id,
                .extended = frame.extended,
                .dlc = frame.dlc,
                .data = frame.dataSlice(),
            }) catch |err| {
                std.debug.print("[Framework Error] CanBridge publish failed: {}\n", .{err});
            };
        }
    }
};

const native_endian = @import("builtin").target.cpu.arch.endian();

fn interfaceIndex(sock: posix.socket_t, interface: []const u8) !i32 {
    var ifr: linux.ifreq = std.mem.zeroes(linux.ifreq);
    const name = ifr.ifrn.name[0 .. linux.IFNAMESIZE - 1];
    const len = @min(interface.len, name.len);
    @memcpy(name[0..len], interface[0..len]);
    try posix.ioctl_SIOCGIFINDEX(sock, &ifr);
    return ifr.ifru.ivalue;
}

test "parseFrame decodes a standard 11-bit id classic CAN frame" {
    // id 0x123, dlc 3, data [1,2,3,0,0,0,0,0]
    var raw: [16]u8 = [_]u8{0} ** 16;
    std.mem.writeInt(u32, raw[0..4], 0x123, .little);
    raw[4] = 3;
    raw[8] = 1;
    raw[9] = 2;
    raw[10] = 3;

    const frame = parseFrame(&raw);
    try std.testing.expectEqual(@as(u32, 0x123), frame.id);
    try std.testing.expect(!frame.extended);
    try std.testing.expectEqual(@as(u8, 3), frame.dlc);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, frame.dataSlice());
}

test "parseFrame decodes an extended 29-bit id" {
    var raw: [16]u8 = [_]u8{0} ** 16;
    std.mem.writeInt(u32, raw[0..4], 0x80012345, .little); // EFF flag set
    raw[4] = 0;

    const frame = parseFrame(&raw);
    try std.testing.expect(frame.extended);
    try std.testing.expectEqual(@as(u32, 0x12345), frame.id);
}
