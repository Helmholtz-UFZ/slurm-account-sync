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
const lib = @import("zig-pwd_lib");
const Runtime = @import("main.zig").Runtime;
const passwd = std.c.passwd;
const uid_t = std.posix.uid_t;
const gid_t = std.posix.gid_t;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.passwd);
const mem = std.mem;
const slurm = @import("slurm");
pub const Entries = std.ArrayListUnmanaged(User);

var rt: *Runtime = undefined;
var group_remaps: GroupRemaps = .{};

const GroupRemapEntry = struct {
    src: []const u8,
    dest: []const u8,
    parent: ?[]const u8 = null,

    pub fn fromString(str: []const u8) GroupRemapEntry {
        var it = std.mem.splitScalar(u8, str, ':');
        const src = it.first();
        const dest = it.next().?;
        const parent = it.next();

        return .{
            .src = src,
            .dest = dest,
            .parent = parent,
        };
    }
};

const GroupRemaps = struct {
    primary_only: std.StringHashMapUnmanaged(GroupRemapEntry) = .empty,
    all: std.StringHashMapUnmanaged(GroupRemapEntry) = .empty,
    specific_users: std.StringHashMapUnmanaged(GroupRemapEntry) = .empty,

    pub fn fromConfig() !GroupRemaps {
        var out: GroupRemaps = .{};

        for (rt.config.group_remap.primary_only) |remap| {
            const entry = GroupRemapEntry.fromString(remap);
            const res = try out.primary_only.getOrPut(rt.allocator, entry.src);
            if (!res.found_existing) {
                res.value_ptr.* = entry;
            }
        }
        for (rt.config.group_remap.all) |remap| {
            const entry = GroupRemapEntry.fromString(remap);
            const res = try out.all.getOrPut(rt.allocator, entry.src);
            if (!res.found_existing) {
                res.value_ptr.* = entry;
            }
        }
        for (rt.config.group_remap.specific_users) |remap| {
            const entry = GroupRemapEntry.fromString(remap);
            const res = try out.specific_users.getOrPut(rt.allocator, entry.src);
            if (!res.found_existing) {
                res.value_ptr.* = entry;
            }
        }

        return out;
    }
};

pub fn getUsers(runtime_data: *Runtime) !Entries {
    rt = runtime_data;
    var entries: Entries = .empty;

    group_remaps = try GroupRemaps.fromConfig();

    while (getpwent()) |entry| {
        var user = User{
            .name = try rt.allocator.dupeZ(u8, std.mem.span(entry.name.?)),
            .uid = entry.uid,
            .gid = entry.gid,
            .parent_account = try rt.allocator.dupeZ(u8, rt.config.default_parent_account),
        };
        try user.assignAccount();
        try entries.append(rt.allocator, user);
    }
    endpwent();

    log.debug("Found {d} Users with getpwent()", .{entries.items.len});
    return entries;
}

pub fn gidToString(allocator: Allocator, gid: gid_t) ?[:0]const u8 {
    var pwd_struct: group = undefined;
    var pwd_result: ?*group = null;
    var buf: [65536:0]u8 = undefined;
    _ = getgrgid_r(gid, &pwd_struct, &buf, buf.len, &pwd_result);

    if (pwd_result) |result| {
        const name = result.name orelse return null;
        return allocator.dupeZ(u8, std.mem.span(name)) catch @panic("OOM");
    } else return null;
}

pub const User = struct {
    name: [:0]const u8,
    uid: uid_t,
    gid: gid_t,
    account: [:0]const u8 = undefined,
    parent_account: [:0]const u8 = undefined,

    fn resolveMainAccount(self: *User) !void {
        const main_group_name = slurm.c.gid_to_string_or_null(self.gid);
        if (main_group_name) |c_grp| {
            self.account = std.mem.span(c_grp);
        } else @panic("Failed to resolve group name");
    }

    fn processSpecificUserRemaps(self: *User) !bool {
        if (group_remaps.specific_users.get(self.name)) |remapped_group| {
            self.account = try rt.allocator.dupeZ(u8, remapped_group.dest);
            self.parent_account = try rt.allocator.dupeZ(u8, remapped_group.parent.?);
            return true;
        } else return false;
    }

    fn processPrimaryGroupRemaps(self: *User) !void {
        if (group_remaps.primary_only.get(self.account)) |remapped_group| {
            self.account = try rt.allocator.dupeZ(u8, remapped_group.dest);
        }
    }

    fn processDepthGroupRemaps(self: *User) !void {
        var ngroups: c_int = 0;
        if (getgrouplist(self.name, self.gid, null, &ngroups) < 0) {
            var group_list = try rt.allocator.alloc(std.c.gid_t, @intCast(ngroups));

            const rc = getgrouplist(self.name, self.gid, group_list.ptr, &ngroups);
            std.debug.assert(rc == group_list.len);

            for (group_list[0..@intCast(ngroups)]) |gid| {
                // TODO: leaking
                const c_grp = slurm.c.gid_to_string_or_null(gid) orelse continue;
                const group_name = std.mem.span(c_grp);

                const maybe_remapped_group = group_remaps.all.get(group_name);
                const is_div = std.mem.startsWith(u8, group_name, "idiv_");

                if (maybe_remapped_group) |remapped_group| {
                    // For now, assume only idiv groups are remapped.
                    self.parent_account = "idiv";
                    self.account = try rt.allocator.dupeZ(u8, remapped_group.dest);
                    break;
                } else if (is_div) {
                    self.parent_account = "idiv";
                    self.account = try rt.allocator.dupeZ(u8, group_name);
                    break;
                }
            }
        }
    }

    pub fn assignAccount(self: *User) !void {
        try self.resolveMainAccount();
        if (try self.processSpecificUserRemaps()) return;
        try self.processDepthGroupRemaps();
        try self.processPrimaryGroupRemaps();
    }
};

pub const group = extern struct {
    name: ?[*:0]const u8 = null,
    password: ?[*:0]const u8 = null,
    gid: std.c.gid_t = 0,
    members: ?*[*:0]const u8 = null,
};

pub extern "c" fn getpwent() ?*passwd;
pub extern "c" fn endpwent() void;
pub extern "c" fn getgrouplist(
    ?[*:0]const u8,
    group: std.c.gid_t,
    groups: ?[*]std.c.gid_t,
    ngroups: *c_int,
) c_int;
pub extern "c" fn getgrnam_r(
    name: [*:0]const u8,
    group: *group,
    buf: [*:0]u8,
    buflen: usize,
    grpretp: *?*group,
) c_int;
pub extern "c" fn getgrgid_r(
    gid: std.c.gid_t,
    group: *group,
    buf: [*:0]u8,
    buflen: usize,
    grpretp: *?*group,
) c_int;
