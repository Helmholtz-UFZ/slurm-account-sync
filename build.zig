const std = @import("std");
const slurm = @import("slurm");
const Allocator = std.mem.Allocator;

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "slurm-account-sync",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    try slurm.setupSlurmPath(b, exe, null);

    const slurm_dep = b.dependency("slurm", .{
        .target = target,
        .optimize = optimize,
    });

    const yazap_dep = b.dependency("yazap", .{});

    const yaml_dep = b.dependency("yaml", .{
        .target = target,
        .optimize = optimize,
    });

    exe.root_module.addImport("yazap", yazap_dep.module("yazap"));
    exe.root_module.addImport("yaml", yaml_dep.module("yaml"));
    exe.root_module.addImport("slurm", slurm_dep.module("slurm"));

    exe.linkLibrary(slurm_dep.artifact("slurm"));

    const exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);

    b.installArtifact(exe);
}
