const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const module = b.addModule("zmysql", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
    });

    module.linkSystemLibrary("mariadb", .{});

    const lib = b.addStaticLibrary(.{
        .name = "zmysql",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    lib.linkLibC();
    lib.linkSystemLibrary("mariadb");

    b.installArtifact(lib);

    const exe = b.addExecutable(.{
        .name = "zmysql",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    exe.linkLibC();
    exe.linkSystemLibrary("mariadb");

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    exe_unit_tests.linkLibC();
    exe_unit_tests.linkSystemLibrary("mariadb");

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);
}
