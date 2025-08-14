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
const Runtime = @import("main.zig").Runtime;
const log = std.log.scoped(.sync);
const Connection = slurm.db.Connection;
const SlurmList = slurm.db.List;
const Account = slurm.db.Account;
const Association = slurm.db.Association;
const User = slurm.db.User;
const checkRpc = slurm.err.checkRpc;
const NoValue = slurm.common.NoValue;
const uid_t = std.posix.uid_t;

var rt: *Runtime = undefined;
const nobody: uid_t = 65534;

pub fn run(runtime_data: *Runtime) void {
    rt = runtime_data;

    createOrganizations();
    addOrModifyUsers();
    deleteOldUsers();
    deleteEmptyAccounts();
}

fn loadAssociations() ?*SlurmList(*Association) {
    const assocs = slurm.db.association.load(rt.db_conn, .{}) catch |err| {
        log.err("Failed to load Associations. Cannot continue cleanup of old Associations", .{});
        log.err("The error was: {s}", .{@errorName(err)});
        return null;
    };
    return assocs;
}

fn findSlurmUser(local_name: [:0]const u8) ?*User {
    for (rt.slurm_users) |slurm_user| {
        if (slurm.parseCStrZ(slurm_user.name)) |slurm_name| {
            if (std.mem.eql(u8, slurm_name, local_name)) return slurm_user;
        }
    }
    return null;
}

fn findLocalUser(slurm_name: [:0]const u8) ?*passwd.User {
    for (rt.local_users.items) |*local_user| {
        if (std.mem.eql(u8, slurm_name, local_user.name)) return local_user;
    }
    return null;
}

fn findAccount(name: []const u8) ?*Account {
    for (rt.slurm_accounts) |account| {
        if (slurm.parseCStrZ(account.name)) |account_name| {
            if (std.mem.eql(u8, account_name, name)) return account;
        }
    }
    return null;
}

fn accountInUse(account_name: [:0]const u8, assocs: *SlurmList(*Association)) bool {
    var assoc_iter = assocs.iter();
    defer assoc_iter.deinit();

    while (assoc_iter.next()) |assoc| {
        if (slurm.parseCStrZ(assoc.account)) |assoc_account| {
            if (assoc.user == null) continue;
            if (std.mem.eql(u8, assoc_account, account_name)) return true;
        }

        if (slurm.parseCStrZ(assoc.parent_acct)) |assoc_parent| {
            if (std.mem.eql(u8, assoc_parent, account_name)) return true;
        }
    }
    return false;
}

fn commitChange() void {
    // No need to call db_conn.rollback() - we just don't need to commit
    // anything
    if (rt.args.dry_run) return;
    rt.db_conn.commit() catch {
        log.err("Failed to commit changes to slurmdbd", .{});
    };
}

fn createOrganizations() void {
    for (rt.config.organizations) |org| {
        if (findAccount(org) != null) continue;
        addAccount(rt.allocator.dupeZ(u8, org) catch @panic("OOM"), "root");
    }
}

fn addOrModifyUsers() void {
    for (rt.local_users.items) |*local_user| {
        if (local_user.uid < rt.config.uid_range.min or
            local_user.uid >= rt.config.uid_range.max or
            local_user.uid == nobody)
            continue;

        if (findSlurmUser(local_user.name)) |slurm_user| {
            modifyUser(local_user, slurm_user);
        } else {
            addUser(local_user);
        }
    }
}

fn modifyUser(local_user: *passwd.User, slurm_user: *User) void {
    // User exists, check for changes
    const def_acct = slurm.parseCStrZ(slurm_user.default_account);
    if (def_acct == null) return;

    if (!std.mem.eql(u8, local_user.account.?, def_acct.?)) {
        // department doesn't match, add new assoc
        addAccount(local_user.account.?, local_user.parent_account);
        addUserAssociation(local_user);
    }
}

fn deleteEmptyAccounts() void {
    const assocs = loadAssociations() orelse return;

    skipOrgDeletion: for (rt.slurm_accounts) |account| {
        const name = slurm.parseCStrZ(account.name);
        if (name == null) {
            log.warn("Encountered Slurm Account with null name, skipping.", .{});
            continue;
        }

        for (rt.config.organizations) |org| {
            if (std.mem.eql(u8, org, name.?)) continue :skipOrgDeletion;
        }

        if (!accountInUse(name.?, assocs) or account.associations == null) {
            deleteAccount(name.?);
        }
    }
}

fn deleteOldUsers() void {
    deleteOldUserAssociations();

    skipDeletion: for (rt.slurm_users) |u| {
        const slurm_name = slurm.parseCStrZ(u.name);
        if (slurm_name == null) {
            log.warn("Encountered Slurm user with null name, skipping.", .{});
            continue;
        }

        for (rt.config.ignore_users_on_delete) |ignore_user| {
            if (std.mem.eql(u8, ignore_user, slurm_name.?)) continue :skipDeletion;
        }

        const local_user = findLocalUser(slurm_name.?);
        if (local_user == null) deleteUser(slurm_name.?);
    }
}

fn deleteOldUserAssociations() void {
    var assocs = loadAssociations() orelse return;
    var assoc_iter = assocs.iter();

    while (assoc_iter.next()) |assoc| {
        const account = slurm.parseCStrZ(assoc.account);
        const user = slurm.parseCStrZ(assoc.user);

        if (account == null or user == null) continue;
        if (assoc.is_def != NoValue.u16 and assoc.is_def > 0) continue;

        var filter: Association.Filter = .{
            .accounts = slurm.db.list.fromCStr(&[_][:0]const u8{
                account.?,
            }),
            .users = slurm.db.list.fromCStr(&[_][:0]const u8{
                user.?,
            }),
        };

        _ = slurm.db.association.removeRaw(rt.db_conn, &filter);
        const err_context = slurm.err.getErrorBundle();

        checkRpc(err_context.code) catch |err| switch (err) {
            error.JobsRunningOnAssoc => {
                log.err("Cannot delete Association (User={s}, Account={s}) yet, because it has still Jobs running.", .{
                    user.?,
                    account.?,
                });
                continue;
            },
            else => {
                log.err("Failed to delete Association: (User={s}, Account={s})", .{
                    user.?,
                    account.?,
                });

                log.err("The error was: {s}", .{err_context.description});
                continue;
            },
        };

        log.info("Deleting unused Association: User={?s} Account={?s}", .{
            user,
            account,
        });

        commitChange();
        rt.change_attempted = true;
    }
}

fn addAccount(name: [:0]const u8, parent: [:0]const u8) void {
    var acct_list: *SlurmList(*Account) = .initWithDestroyFunc(null);
    defer acct_list.deinit();

    var account: Account = .{
        .name = name,
        .organization = parent,
        .description = name,
    };
    acct_list.append(&account);
    slurm.db.account.add(rt.db_conn, acct_list) catch {
        log.err("Failed to add Account: {s}", .{name});
        return;
    };

    var assoc: Association = .{
        .account = name,
        .parent_acct = parent,
    };
    addAssociation(&assoc);
}

fn deleteAccount(account: [:0]const u8) void {
    var assoc_filter: Association.Filter = .{
        .accounts = slurm.db.list.fromCStr(&[_][:0]const u8{account}),
    };

    var filter: Account.Filter = .{
        .association_filter = &assoc_filter,
    };

    _ = slurm.db.account.removeRaw(rt.db_conn, &filter);
    const err_context = slurm.err.getErrorBundle();

    checkRpc(err_context.code) catch |err| {
        log.err("Cannot delete Account: {s}", .{account});
        log.err("The error was: ", .{});
        log.err("{s}({d}): {s}", .{
            @errorName(err),
            err_context.code,
            err_context.description,
        });
        return;
    };

    log.info("Deleted account: {s}", .{account});
    commitChange();
    rt.change_attempted = true;
}

fn tresListToKeyValue(list: []const []const u8) !?[*:0]const u8 {
    var str: std.ArrayListUnmanaged(u8) = .empty;

    for (list) |item| {
        // TODO: fetch TRES and convert string tres to its ID
        try str.appendSlice(rt.allocator, item);
    }

    const slice = try str.toOwnedSliceSentinel(rt.allocator, 0);
    return slice.ptr;
}

fn addAssociation(assoc: *Association) void {
    var assoc_list: *SlurmList(*Association) = .initWithDestroyFunc(null);
    defer assoc_list.deinit();

    const is_user_assoc = assoc.user != null;

    assoc.cluster = rt.cluster;

    if (rt.config.limits) |limits| {
        if (is_user_assoc) {
            assoc.is_def = 1;
            assoc.shares_raw = limits.shares orelse NoValue.u32;
            assoc.max_submit_jobs = limits.max_submit_jobs orelse NoValue.u32;
            assoc.grp_tres_run_mins = tresListToKeyValue(limits.grp_tres_run_mins) catch null;
            assoc.grp_tres = tresListToKeyValue(limits.grp_tres) catch null;
        }
    }

    assoc_list.append(assoc);

    const assoc_fmt = "User={?s} Account={?s} Cluster={?s}";
    const print_args = .{
        if (is_user_assoc) assoc.user else "N/A",
        assoc.account,
        assoc.cluster,
    };

    slurm.db.association.add(rt.db_conn, assoc_list) catch {
        log.err("Failed to add Association" ++ assoc_fmt, print_args);
        return;
    };
    log.info("Added Association: User={?s} Account={?s} Cluster={?s}", print_args);

    commitChange();
    rt.change_attempted = true;
}

fn addUser(user: *passwd.User) void {
    addAccount(user.account.?, user.parent_account);

    var user_list: *SlurmList(*User) = .initWithDestroyFunc(null);
    defer user_list.deinit();

    var slurm_user: User = .{
        .name = user.name,
        .default_account = user.account.?,
    };

    user_list.append(&slurm_user);
    slurm.db.user.add(rt.db_conn, user_list) catch {
        log.err("slurm.db.user.add() failed for User {s}", .{user.name});
        return;
    };
    addUserAssociation(user);
}

fn deleteUser(user: [:0]const u8) void {
    var assoc_filter: Association.Filter = .{
        .users = slurm.db.list.fromCStr(&[_][:0]const u8{user}),
    };

    var filter: User.Filter = .{ .association_filter = &assoc_filter };

    _ = slurm.db.user.removeRaw(rt.db_conn, &filter);
    const err_context = slurm.err.getErrorBundle();

    checkRpc(err_context.code) catch |err| {
        log.err("Cannot delete User: {s}", .{user});
        log.err("The error was: ", .{});
        log.err("{s}({d}): {s}", .{
            @errorName(err),
            err_context.code,
            err_context.description,
        });
        return;
    };

    log.info("Deleted user: {s}", .{user});
    commitChange();
    rt.change_attempted = true;
}

fn addUserAssociation(user: *passwd.User) void {
    var assoc: Association = .{
        .user = user.name,
        .account = user.account.?,
        .parent_acct = user.parent_account,
    };
    addAssociation(&assoc);
}
