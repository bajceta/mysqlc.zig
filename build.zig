const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const module = b.addModule("zmysql", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .link_libc = true,
    });

    module.linkSystemLibrary("mariadb", .{});

    const exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    exe_unit_tests.linkLibC();
    exe_unit_tests.linkSystemLibrary("libmariadb");

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);
}
