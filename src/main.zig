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

const std = @import("std");
const slurm = @import("slurm");
const passwd = @import("passwd.zig");
const Config = @import("Config.zig");
const Args = @import("Args.zig");
const sync = @import("sync.zig");
const log = std.log.scoped(.main);

pub const std_options: std.Options = .{
    .log_level = .debug,
    .logFn = logFn,
};

var rt: Runtime = .{};

pub const Runtime = struct {
    allocator: std.mem.Allocator = undefined,
    db_conn: *slurm.db.Connection = undefined,
    local_users: passwd.Entries = .empty,
    slurm_users: []const *slurm.db.User = &.{},
    slurm_accounts: []const *slurm.db.Account = &.{},
    args: Args = .{},
    config: Config = .{ .cluster = "UNKNOWN" },
    mail_message: std.ArrayListUnmanaged(u8) = .empty,
    change_attempted: bool = false,
    cluster: [:0]const u8 = undefined,
    tres: []const *slurm.db.TrackableResource = &.{},

    private: struct {
        slurm_accounts_list: *slurm.db.List(*slurm.db.Account) = undefined,
        slurm_users_list: *slurm.db.List(*slurm.db.User) = undefined,
        slurm_tres_list: *slurm.db.List(*slurm.db.TrackableResource) = undefined,
    } = .{},

    pub fn init(allocator: std.mem.Allocator) !void {
        rt.allocator = allocator;
        rt.args = try .parse(rt.allocator);

        if (rt.args.config_path) |config_path| {
            rt.config = try Config.parse(rt.allocator, config_path);
        }

        rt.cluster = blk: {
            if (rt.args.cluster) |cluster| {
                break :blk try rt.allocator.dupeZ(u8, cluster);
            }

            if (!std.mem.eql(u8, rt.config.cluster, "UNKNOWN")) {
                break :blk try rt.allocator.dupeZ(u8, rt.config.cluster);
            }

            log.err("You must configure a Cluster", .{});
            std.process.exit(1);
        };

        rt.local_users = try passwd.getUsers(&rt);

        slurm.init(null);
        rt.db_conn = slurm.db.Connection.open() catch {
            log.err("Failed to open Connection to slurmdbd, aborting...", .{});
            std.process.exit(1);
        };

        rt.private.slurm_users_list = try slurm.db.user.load(rt.db_conn, .{
            .with_assocs = 1,
        });
        rt.slurm_users = try rt.private.slurm_users_list.constSlice(rt.allocator);

        rt.private.slurm_accounts_list = try slurm.db.account.load(rt.db_conn, .{
            .flags = .{
                .with_assocs = true,
            },
        });
        rt.slurm_accounts = try rt.private.slurm_accounts_list.constSlice(rt.allocator);

        rt.private.slurm_tres_list = try slurm.db.tres.load(rt.db_conn, .{});
        rt.tres = try rt.private.slurm_tres_list.constSlice(rt.allocator);
    }

    pub fn deinit(self: *Runtime) void {
        self.private.slurm_users_list.deinit();
        self.private.slurm_accounts_list.deinit();
        self.private.slurm_tres_list.deinit();
        self.db_conn.close();
        slurm.deinit();
    }
};

pub fn main() !void {
    var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    try Runtime.init(allocator);
    defer rt.deinit();

    if (rt.args.dry_run) {
        log.info("dry-run is enabled - no changes will be committed", .{});
    }

    if (rt.config.limits) |limits| {
        log.info(
            "Current Limits:\n{s}",
            .{std.json.fmt(limits, .{
                .whitespace = .indent_4,
                .emit_null_optional_fields = false,
            })},
        );
    }

    log.info("Running sync for Cluster: {s}", .{rt.cluster});

    sync.run(&rt);
    try sendMail();
}

fn sendMail() !void {
    if (!rt.args.send_mail or !rt.change_attempted) return;
    const mail_opts = rt.config.mail_options orelse return;

    var mail: std.process.Child = .init(
        &.{ "/usr/bin/mailx", "-Ssendwait", "-t" },
        rt.allocator,
    );

    mail.stdin_behavior = .Pipe;
    mail.stdout_behavior = .Ignore;
    mail.stderr_behavior = .Ignore;

    try mail.spawn();
    const stdin = mail.stdin.?;
    const writer = stdin.writer();

    if (mail_opts.from) |from| {
        try writer.print("From: {s}\n", .{from});
    }
    try writer.print("To: {s}\n", .{mail_opts.to});

    const subject_prefix = if (rt.args.dry_run) "[DRY-RUN] " else "";
    try writer.print("Subject: {s}{s}\n\n", .{ subject_prefix, mail_opts.subject });

    try writer.writeAll(rt.mail_message.items);

    log.info("Sending mail to {s}", .{mail_opts.to});

    stdin.close();
    mail.stdin = null;
    _ = try mail.wait();
}

pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    log_args: anytype,
) void {
    if (@intFromEnum(level) > @intFromEnum(std.log.Level.info) and
        !rt.args.debug)
        return;

    const scope_name = switch (scope) {
        .main, .sync, .config, .passwd, .gpa => @tagName(scope),
        else => return,
    };

    const level_prefix = comptime level.asText();
    const scope_prefix = if (scope == .default) ": " else "(" ++ scope_name ++ "): ";

    const fmt = level_prefix ++ scope_prefix ++ format ++ "\n";
    std.debug.print(fmt, log_args);

    if (rt.args.send_mail and rt.config.mail_options != null) {
        const data = std.fmt.allocPrint(rt.allocator, fmt, log_args) catch return;
        rt.mail_message.appendSlice(rt.allocator, data) catch return;
    }
}
