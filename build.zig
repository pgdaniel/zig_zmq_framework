const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const framework_mod = b.addModule("zig_zmq_framework", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Prefer the DLL import library over the static archive to avoid pulling
    // in GCC's C++ runtime (libstdc++) into the linker command.
    framework_mod.linkSystemLibrary("zmq", .{ .prefer_static = false });
    framework_mod.link_libc = true;

    // Tests live as `test {}` blocks inside the library source files.
    const tests_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    tests_mod.linkSystemLibrary("zmq", .{});
    const lib_tests = b.addTest(.{
        .root_module = tests_mod,
    });

    const run_lib_tests = b.addRunArtifact(lib_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);
}
