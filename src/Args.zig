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
const yazap = @import("yazap");
const App = yazap.App;
const Arg = yazap.Arg;
const Args = @This();
const Allocator = std.mem.Allocator;

const options: App.Options = .{
    .display_help = true,
    .help_options = .{
        .header_styles = .{
            .bold = true,
            .underlined = true,
        },
        .line_style = .{
            .format = .description_separate,
            .signature = .{
                .left_padding = 2,
                .max_width = 40,
            },
            .description = .{
                .left_padding = 10,
            },
        },
    },
};

_app: App = undefined,
send_mail: bool = false,
dry_run: bool = false,
debug: bool = false,
config_path: ?[]const u8 = null,
cluster: ?[]const u8 = null,

pub fn parse(allocator: Allocator) !Args {
    var app = try setupApp(allocator);
    const matches = try app.parseProcess();

    var args: Args = .{
        .send_mail = matches.containsArg("send-mail"),
        .dry_run = matches.containsArg("dry-run"),
        .debug = matches.containsArg("debug"),
        ._app = app,
    };

    if (matches.getSingleValue("config")) |arg| {
        args.config_path = arg;
    }

    if (matches.getSingleValue("cluster")) |arg| {
        args.cluster = arg;
    }

    return args;
}

fn setupApp(allocator: Allocator) !App {
    var app = App.initWithOptions(
        allocator,
        "slurm-account-sync",
        "Sync Users and Accounts from the local database to Slurm",
        options,
    );

    var root_cmd = app.rootCommand();

    try root_cmd.addArgs(&[_]Arg{
        Arg.booleanOption(
            "send-mail",
            'm',
            \\Send an E-Mail.
            ,
        ),
        Arg.booleanOption(
            "dry-run",
            'd',
            \\Only show what would change, but don't commit anything.
            ,
        ),
        Arg.booleanOption(
            "debug",
            null,
            \\Enable debug-logging.
            ,
        ),
        Arg.singleValueOption(
            "config",
            'c',
            "Path to a config file.",
        ),
        Arg.singleValueOption(
            "cluster",
            'C',
            "Name of the Cluster to operate on.",
        ),
    });

    return app;
}
