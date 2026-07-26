const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Shared module: the framework itself (bus, node contract, flow parser,
    // state registry, CAN bridge). Every executable below imports this.
    const framework_mod = b.addModule("zig_zmq_framework", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    framework_mod.linkSystemLibrary("zmq", .{});
    framework_mod.link_libc = true;

    const exes = [_]struct { name: []const u8, src: []const u8 }{
        .{ .name = "flowctl", .src = "src/flowctl.zig" },
        .{ .name = "ecu", .src = "src/nodes/ecu.zig" },
        .{ .name = "telemetry", .src = "src/nodes/telemetry.zig" },
        .{ .name = "dashboard", .src = "src/nodes/dashboard.zig" },
        .{ .name = "webapp", .src = "src/nodes/webapp.zig" },
        .{ .name = "state_registry", .src = "src/nodes/state_registry_node.zig" },
        .{ .name = "can_bridge", .src = "src/nodes/can_bridge_node.zig" },
    };

    const install_step = b.getInstallStep();
    const run_all = b.step("run-check", "build only (no run) — sanity target");
    _ = run_all;

    inline for (exes) |exe_spec| {
        const exe = b.addExecutable(.{
            .name = exe_spec.name,
            .root_source_file = b.path(exe_spec.src),
            .target = target,
            .optimize = optimize,
        });
        exe.root_module.addImport("zig_zmq_framework", framework_mod);
        exe.linkSystemLibrary("zmq");
        exe.linkLibC();
        b.installArtifact(exe);
        install_step.dependOn(&exe.step);

        const run_cmd = b.addRunArtifact(exe);
        run_cmd.step.dependOn(b.getInstallStep());
        if (b.args) |args| run_cmd.addArgs(args);
        const run_step = b.step(b.fmt("run-{s}", .{exe_spec.name}), b.fmt("Run {s}", .{exe_spec.name}));
        run_step.dependOn(&run_cmd.step);
    }

    // Tests live as `test {}` blocks inside the library source files.
    const lib_tests = b.addTest(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_tests.linkSystemLibrary("zmq");
    lib_tests.linkLibC();

    const run_lib_tests = b.addRunArtifact(lib_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);
}
