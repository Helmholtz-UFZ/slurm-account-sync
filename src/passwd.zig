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
pub const Entries = std.ArrayListUnmanaged(User);

var rt: *Runtime = undefined;
var group_remaps: std.StringHashMapUnmanaged([]const u8) = .empty;

pub fn getUsers(runtime_data: *Runtime) !Entries {
    rt = runtime_data;
    var entries: Entries = .empty;

    for (rt.config.group_remap) |remap| {
        var it = std.mem.splitScalar(u8, remap, ':');
        const res = try group_remaps.getOrPut(rt.allocator, it.first());
        if (!res.found_existing) {
            res.value_ptr.* = it.rest();
        }
    }

    while (getpwent()) |entry| {
        var user = User{
            .name = try rt.allocator.dupeZ(u8, std.mem.span(entry.name.?)),
            .uid = entry.uid,
            .gid = entry.gid,
            .parent_account = "ufz",
        };
        try user.assignAccount(rt.allocator);
        try entries.append(rt.allocator, user);
    }
    endpwent();

    log.debug("Found {d} Users in local passwd database", .{entries.items.len});
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
    account: ?[:0]const u8 = null,
    parent_account: [:0]const u8 = undefined,

    fn checkForiDivAccount(self: *User, allocator: Allocator) !?[:0]const u8 {
        var ngroups: c_int = 0;
        if (getgrouplist(self.name, self.gid, null, &ngroups) < 0) {
            var group_list = try allocator.alloc(std.c.gid_t, @intCast(ngroups));

            const rc = getgrouplist(self.name, self.gid, group_list.ptr, &ngroups);
            std.debug.assert(rc == group_list.len);

            for (group_list[0..@intCast(ngroups)]) |gid| {
                const group_name = gidToString(allocator, gid) orelse continue;

                if (!std.mem.startsWith(u8, group_name, "idiv_")) continue;

                self.parent_account = "idiv";
                if (group_remaps.get(group_name)) |remapped_group| {
                    return try allocator.dupeZ(u8, remapped_group);
                }

                return group_name;
            }
        }
        return null;
    }

    pub fn assignAccount(self: *User, allocator: Allocator) !void {
        if (try self.checkForiDivAccount(allocator)) |idiv_group| {
            self.account = idiv_group;
        } else self.account = gidToString(allocator, self.gid);
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
