//  slurm-account-sync: Synchronise local users to the Slurm database.
//
//  Copyright (C) 2025 Helmholtz Centre for Environmental Research GmbH - UFZ
//  Written by Toni Harzendorf <toni.harzendorf@ufz.de>
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.

//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.

//  You should have received a copy of the GNU General Public License
//  along with this program. If not, see <https://www.gnu.org/licenses/>.

const slurm = @import("slurm");
const std = @import("std");
const Yaml = @import("yaml").Yaml;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.config);
const uid_t = std.posix.uid_t;
const Config = @This();

cluster: []const u8,
ignore_users_on_delete: []const []const u8 = &.{},
organizations: []const []const u8 = &.{},
group_remap: []const []const u8 = &.{},
uid_range: struct {
    min: uid_t = 1000,
    max: uid_t = 100000,
} = .{},
limits: ?struct {
    shares: ?u32 = null,
    max_submit_jobs: ?u32 = null,
    grp_tres_run_mins: []const []const u8 = &.{},
    grp_tres: []const []const u8 = &.{},
} = .{},
mail_options: ?struct {
    from: ?[]const u8 = null,
    to: []const u8,
    subject: []const u8,
} = null,

pub fn parse(allocator: Allocator, config_path: []const u8) !Config {
    const file = try std.fs.cwd().openFile(config_path, .{});
    defer file.close();

    const source = try file.readToEndAlloc(allocator, std.math.maxInt(u32));

    var yaml: Yaml = .{ .source = source };
    defer yaml.deinit(allocator);

    try yaml.load(allocator);
    const config: Config = try yaml.parse(allocator, Config);

    log.info("Loaded config: \"{s}\"", .{config_path});
    log.debug("Content of \"{s}\":\n{s}", .{ config_path, source });
    return config;
}
