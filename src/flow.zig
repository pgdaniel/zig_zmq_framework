//! Parses a flow manifest (flow.yml) — the graph as data, Node-RED style.
//! The manifest is the ONLY place that knows the topology; node processes
//! learn their wiring from environment variables computed here (see
//! `wiring`), and a node's code never mentions another node.
//!
//! Only the small subset of YAML flow.yml actually uses is supported:
//! a top-level `nodes:` map, one 2-space-indented block per node, each with
//! `cmd:` (scalar), `publishes:`/`subscribes:` (flow lists `[a, b]`), and an
//! optional `env:` (flow map `{ K: "v" }`). That's deliberate — the whole
//! point of the manifest is to stay this simple.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const FlowError = error{
    MissingNodesKey,
    MissingCmd,
};

pub const Diagnostics = struct {
    buf: [256]u8 = undefined,
    message: []const u8 = "",

    fn set(self: *Diagnostics, comptime fmt: []const u8, args: anytype) void {
        self.message = std.fmt.bufPrint(&self.buf, fmt, args) catch self.buf[0..];
    }
};

pub const EnvPair = struct { key: []const u8, value: []const u8 };

pub const Node = struct {
    name: []const u8,
    cmd: []const u8,
    publishes: [][]const u8,
    subscribes: [][]const u8,
    env: []EnvPair,
};

pub const WiringEntry = struct {
    node_name: []const u8,
    env: []EnvPair,
};

pub const Flow = struct {
    arena: std.heap.ArenaAllocator,
    nodes: []Node,

    pub fn loadFile(gpa: Allocator, path: []const u8, diag: ?*Diagnostics) !Flow {
        const data = try std.fs.cwd().readFileAlloc(gpa, path, 10 * 1024 * 1024);
        defer gpa.free(data);
        return parseText(gpa, data, diag);
    }

    pub fn parseText(gpa: Allocator, text: []const u8, diag: ?*Diagnostics) !Flow {
        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();

        const nodes = try parseNodes(a, text, diag);
        var self = Flow{ .arena = arena, .nodes = nodes };
        self.warnAboutDeafSubscriptions();
        return self;
    }

    pub fn deinit(self: *Flow) void {
        self.arena.deinit();
    }

    /// The environment for every node process, given a {name => port} map.
    /// This is the whole trick that keeps nodes blackboxes: each node's
    /// peer list is computed from who publishes the topics it subscribes to.
    pub fn wiring(self: *const Flow, allocator: Allocator, ports: std.StringHashMap(u16)) ![]WiringEntry {
        var out = std.ArrayList(WiringEntry).init(allocator);
        for (self.nodes) |node| {
            const peers = try self.peerNames(allocator, node);
            defer allocator.free(peers);

            var peer_str = std.ArrayList(u8).init(allocator);
            defer peer_str.deinit();
            for (peers, 0..) |peer_name, i| {
                if (i != 0) try peer_str.appendSlice(",");
                const port = ports.get(peer_name) orelse return error.UnknownPeerPort;
                try peer_str.writer().print("127.0.0.1:{d}", .{port});
            }

            var env_list = std.ArrayList(EnvPair).init(allocator);
            const my_port = ports.get(node.name) orelse return error.UnknownPeerPort;
            try env_list.append(.{ .key = "BUS_PORT", .value = try std.fmt.allocPrint(allocator, "{d}", .{my_port}) });
            try env_list.append(.{ .key = "BUS_PEERS", .value = try allocator.dupe(u8, peer_str.items) });
            try env_list.append(.{ .key = "BUS_SUBSCRIBES", .value = try joinComma(allocator, node.subscribes) });
            try env_list.append(.{ .key = "NODE_NAME", .value = try allocator.dupe(u8, node.name) });
            for (node.env) |pair| try env_list.append(pair);

            try out.append(.{ .node_name = node.name, .env = try env_list.toOwnedSlice() });
        }
        return out.toOwnedSlice();
    }

    /// The node-to-node topology, for visualization (flowctl --graph).
    /// Every topic a node subscribes to becomes an edge from each of its
    /// publishers, except heartbeat (implicit, all-to-all) and topics
    /// nobody publishes (surfaced as "unresolved" instead of a dangling edge).
    pub fn graph(self: *const Flow, allocator: Allocator) !std.json.Value {
        var nodes_arr = std.json.Array.init(allocator);
        for (self.nodes) |node| {
            var obj = std.json.ObjectMap.init(allocator);
            try obj.put("name", .{ .string = node.name });
            try obj.put("cmd", .{ .string = node.cmd });
            try obj.put("publishes", try stringArrayToJson(allocator, node.publishes));
            try obj.put("subscribes", try stringArrayToJson(allocator, node.subscribes));
            var env_obj = std.json.ObjectMap.init(allocator);
            for (node.env) |pair| try env_obj.put(pair.key, .{ .string = pair.value });
            try obj.put("env", .{ .object = env_obj });
            try nodes_arr.append(.{ .object = obj });
        }

        var edges_arr = std.json.Array.init(allocator);
        var unresolved_arr = std.json.Array.init(allocator);

        for (self.nodes) |node| {
            for (node.subscribes) |topic| {
                if (std.mem.eql(u8, topic, "heartbeat")) continue;

                const publishers = try self.publisherNames(allocator, topic, node.name);
                defer allocator.free(publishers);

                if (publishers.len == 0) {
                    var u = std.json.ObjectMap.init(allocator);
                    try u.put("topic", .{ .string = topic });
                    try u.put("to", .{ .string = node.name });
                    try unresolved_arr.append(.{ .object = u });
                } else {
                    for (publishers) |from| {
                        var e = std.json.ObjectMap.init(allocator);
                        try e.put("from", .{ .string = from });
                        try e.put("to", .{ .string = node.name });
                        try e.put("topic", .{ .string = topic });
                        try edges_arr.append(.{ .object = e });
                    }
                }
            }
        }

        var root = std.json.ObjectMap.init(allocator);
        try root.put("nodes", .{ .array = nodes_arr });
        try root.put("edges", .{ .array = edges_arr });
        try root.put("unresolved", .{ .array = unresolved_arr });
        return .{ .object = root };
    }

    /// Every node broadcasts :heartbeat implicitly, so for that topic
    /// everyone counts as a publisher. A node never peers with itself — the
    /// bus already delivers its own publishes locally.
    fn publisherNames(self: *const Flow, allocator: Allocator, topic: []const u8, exclude: []const u8) ![][]const u8 {
        var out = std.ArrayList([]const u8).init(allocator);
        if (std.mem.eql(u8, topic, "heartbeat")) {
            for (self.nodes) |n| {
                if (!std.mem.eql(u8, n.name, exclude)) try out.append(n.name);
            }
            return out.toOwnedSlice();
        }
        for (self.nodes) |n| {
            if (std.mem.eql(u8, n.name, exclude)) continue;
            for (n.publishes) |p| {
                if (std.mem.eql(u8, p, topic)) {
                    try out.append(n.name);
                    break;
                }
            }
        }
        return out.toOwnedSlice();
    }

    fn peerNames(self: *const Flow, allocator: Allocator, node: Node) ![][]const u8 {
        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();
        var out = std.ArrayList([]const u8).init(allocator);

        for (node.subscribes) |topic| {
            const publishers = try self.publisherNames(allocator, topic, node.name);
            defer allocator.free(publishers);
            for (publishers) |name| {
                const gop = try seen.getOrPut(name);
                if (!gop.found_existing) try out.append(name);
            }
        }
        return out.toOwnedSlice();
    }

    fn warnAboutDeafSubscriptions(self: *const Flow) void {
        for (self.nodes) |node| {
            outer: for (node.subscribes) |topic| {
                if (std.mem.eql(u8, topic, "heartbeat")) continue;
                for (self.nodes) |n| {
                    for (n.publishes) |p| {
                        if (std.mem.eql(u8, p, topic)) continue :outer;
                    }
                }
                std.debug.print(
                    "[Framework Warning] {s} subscribes to \"{s}\" but no node in the flow publishes it\n",
                    .{ node.name, topic },
                );
            }
        }
    }
};

fn joinComma(allocator: Allocator, items: []const []const u8) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    for (items, 0..) |item, i| {
        if (i != 0) try buf.appendSlice(",");
        try buf.appendSlice(item);
    }
    return buf.toOwnedSlice();
}

fn stringArrayToJson(allocator: Allocator, items: []const []const u8) !std.json.Value {
    var arr = std.json.Array.init(allocator);
    for (items) |item| try arr.append(.{ .string = item });
    return .{ .array = arr };
}

fn stripQuotes(s: []const u8) []const u8 {
    if (s.len >= 2 and ((s[0] == '"' and s[s.len - 1] == '"') or (s[0] == '\'' and s[s.len - 1] == '\''))) {
        return s[1 .. s.len - 1];
    }
    return s;
}

fn indentOf(line: []const u8) usize {
    var n: usize = 0;
    while (n < line.len and line[n] == ' ') : (n += 1) {}
    return n;
}

fn isBlankOrComment(trimmed: []const u8) bool {
    return trimmed.len == 0 or trimmed[0] == '#';
}

/// Splits "key: value" (or "key:" with an empty value) at the first
/// colon-then-space (or a trailing colon). Values may contain their own
/// colons (e.g. a URL) without being mistaken for a new key.
fn splitKeyValue(trimmed: []const u8) ?struct { key: []const u8, value: []const u8 } {
    var i: usize = 0;
    while (i < trimmed.len) : (i += 1) {
        if (trimmed[i] != ':') continue;
        if (i + 1 == trimmed.len) return .{ .key = trimmed[0..i], .value = "" };
        if (trimmed[i + 1] == ' ') return .{ .key = trimmed[0..i], .value = std.mem.trim(u8, trimmed[i + 1 ..], " \t") };
    }
    return null;
}

fn parseFlowList(allocator: Allocator, value: []const u8) ![][]const u8 {
    var out = std.ArrayList([]const u8).init(allocator);
    if (value.len < 2 or value[0] != '[' or value[value.len - 1] != ']') return out.toOwnedSlice();
    const inner = value[1 .. value.len - 1];
    var it = std.mem.splitScalar(u8, inner, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (trimmed.len == 0) continue;
        try out.append(try allocator.dupe(u8, stripQuotes(trimmed)));
    }
    return out.toOwnedSlice();
}

fn parseFlowMap(allocator: Allocator, value: []const u8) ![]EnvPair {
    var out = std.ArrayList(EnvPair).init(allocator);
    if (value.len < 2 or value[0] != '{' or value[value.len - 1] != '}') return out.toOwnedSlice();
    const inner = value[1 .. value.len - 1];
    var it = std.mem.splitScalar(u8, inner, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t");
        if (trimmed.len == 0) continue;
        const kv = splitKeyValue(trimmed) orelse continue;
        try out.append(.{
            .key = try allocator.dupe(u8, std.mem.trim(u8, kv.key, " \t")),
            .value = try allocator.dupe(u8, stripQuotes(kv.value)),
        });
    }
    return out.toOwnedSlice();
}

const Building = struct {
    name: []const u8,
    cmd: []const u8 = "",
    has_cmd: bool = false,
    publishes: [][]const u8 = &.{},
    subscribes: [][]const u8 = &.{},
    env: []EnvPair = &.{},
};

fn finish(a: Allocator, out: *std.ArrayList(Node), building: Building, diag: ?*Diagnostics) !void {
    if (!building.has_cmd) {
        if (diag) |d| d.set("[Framework Error] Flow node {s} needs a cmd", .{building.name});
        return FlowError.MissingCmd;
    }
    try out.append(.{
        .name = building.name,
        .cmd = building.cmd,
        .publishes = building.publishes,
        .subscribes = building.subscribes,
        .env = building.env,
    });
    _ = a;
}

fn parseNodes(a: Allocator, text: []const u8, diag: ?*Diagnostics) ![]Node {
    var lines = std.mem.splitScalar(u8, text, '\n');

    // Find the top-level `nodes:` key.
    var found_nodes_key = false;
    while (lines.next()) |raw| {
        const line = std.mem.trimRight(u8, raw, "\r");
        const trimmed = std.mem.trim(u8, line, " \t");
        if (indentOf(line) != 0) continue;
        if (isBlankOrComment(trimmed)) continue;
        if (std.mem.eql(u8, trimmed, "nodes:")) {
            found_nodes_key = true;
            break;
        }
        // Some other top-level key before `nodes:` — keep scanning.
    }
    if (!found_nodes_key) {
        if (diag) |d| d.set("[Framework Error] Flow manifest needs a top-level \"nodes\" map", .{});
        return FlowError.MissingNodesKey;
    }

    var out = std.ArrayList(Node).init(a);
    var node_indent: ?usize = null;
    var field_indent: ?usize = null;
    var building: ?Building = null;

    while (lines.next()) |raw| {
        const line = std.mem.trimRight(u8, raw, "\r");
        const trimmed = std.mem.trim(u8, line, " \t");
        if (isBlankOrComment(trimmed)) continue;

        const indent = indentOf(line);
        if (indent == 0) break; // next top-level key: nodes section is over

        if (node_indent == null) node_indent = indent;

        if (indent == node_indent.?) {
            // A new node header, e.g. "  ecu:".
            if (building) |b| try finish(a, &out, b, diag);
            const name = if (trimmed[trimmed.len - 1] == ':') trimmed[0 .. trimmed.len - 1] else trimmed;
            building = Building{ .name = try a.dupe(u8, name) };
            field_indent = null;
            continue;
        }

        if (field_indent == null) field_indent = indent;
        if (indent != field_indent.? or building == null) continue;

        const kv = splitKeyValue(trimmed) orelse continue;
        const key = std.mem.trim(u8, kv.key, " \t");
        var b = &building.?;
        if (std.mem.eql(u8, key, "cmd")) {
            b.cmd = try a.dupe(u8, stripQuotes(kv.value));
            b.has_cmd = true;
        } else if (std.mem.eql(u8, key, "publishes")) {
            b.publishes = try parseFlowList(a, kv.value);
        } else if (std.mem.eql(u8, key, "subscribes")) {
            b.subscribes = try parseFlowList(a, kv.value);
        } else if (std.mem.eql(u8, key, "env")) {
            b.env = try parseFlowMap(a, kv.value);
        }
    }
    if (building) |b| try finish(a, &out, b, diag);

    return out.toOwnedSlice();
}

test "wiring computes peers from topic publishers" {
    const spec =
        \\nodes:
        \\  ecu:
        \\    cmd: ruby nodes/ecu.rb
        \\    publishes: [engine_data]
        \\    subscribes: [throttle_request]
        \\
        \\  telemetry:
        \\    cmd: ruby nodes/telemetry.rb
        \\    publishes: [throttle_request]
        \\    subscribes: [engine_data]
        \\
        \\  registry:
        \\    cmd: ruby nodes/state_registry.rb
        \\    subscribes: [heartbeat, engine_data]
        \\    env: { VERBOSE: "1" }
        \\
    ;
    var flow = try Flow.parseText(std.testing.allocator, spec, null);
    defer flow.deinit();

    var ports = std.StringHashMap(u16).init(std.testing.allocator);
    defer ports.deinit();
    try ports.put("ecu", 5001);
    try ports.put("telemetry", 5002);
    try ports.put("registry", 5003);

    const w = try flow.wiring(std.testing.allocator, ports);
    defer std.testing.allocator.free(w);

    var by_name = std.StringHashMap(WiringEntry).init(std.testing.allocator);
    defer by_name.deinit();
    for (w) |entry| try by_name.put(entry.node_name, entry);

    const ecu_env = by_name.get("ecu").?.env;
    try std.testing.expectEqualStrings("BUS_PORT", ecu_env[0].key);
    try std.testing.expectEqualStrings("5001", ecu_env[0].value);
    try std.testing.expectEqualStrings("127.0.0.1:5002", ecu_env[1].value);
    try std.testing.expectEqualStrings("ecu", ecu_env[3].value);

    const registry_env = by_name.get("registry").?.env;
    try std.testing.expectEqualStrings("heartbeat,engine_data", registry_env[2].value);
    try std.testing.expectEqualStrings("VERBOSE", registry_env[4].key);
    try std.testing.expectEqualStrings("1", registry_env[4].value);

    // heartbeat makes every node a publisher except yourself
    const registry_peers = registry_env[1].value;
    try std.testing.expect(std.mem.indexOf(u8, registry_peers, "5001") != null);
    try std.testing.expect(std.mem.indexOf(u8, registry_peers, "5002") != null);
}

test "a node without cmd is rejected with a diagnostic" {
    const spec =
        \\nodes:
        \\  broken:
        \\    publishes: [x]
        \\
    ;
    var diag = Diagnostics{};
    const result = Flow.parseText(std.testing.allocator, spec, &diag);
    try std.testing.expectError(FlowError.MissingCmd, result);
    try std.testing.expect(std.mem.indexOf(u8, diag.message, "broken") != null);
}

test "a manifest without a nodes key is rejected" {
    var diag = Diagnostics{};
    const result = Flow.parseText(std.testing.allocator, "not_nodes: {}\n", &diag);
    try std.testing.expectError(FlowError.MissingNodesKey, result);
}

test "graph surfaces unpublished topics as unresolved instead of an edge" {
    const spec =
        \\nodes:
        \\  lonely:
        \\    cmd: "true"
        \\    subscribes: [ghost_topic]
        \\
    ;
    var flow = try Flow.parseText(std.testing.allocator, spec, null);
    defer flow.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const g = try flow.graph(arena.allocator());

    const unresolved = g.object.get("unresolved").?.array;
    try std.testing.expectEqual(@as(usize, 1), unresolved.items.len);
    try std.testing.expectEqualStrings("ghost_topic", unresolved.items[0].object.get("topic").?.string);
    try std.testing.expectEqualStrings("lonely", unresolved.items[0].object.get("to").?.string);

    const edges = g.object.get("edges").?.array;
    try std.testing.expectEqual(@as(usize, 0), edges.items.len);
}
