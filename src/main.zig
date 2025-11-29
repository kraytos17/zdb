const std = @import("std");
const fs = std.fs;

const Db = @import("db.zig").Db;
const print = std.debug.print;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const cwd = std.fs.cwd();
    print("Opening DB...\n", .{});
    var db = try Db.open(cwd, "demo.db", allocator);
    defer db.close() catch {};

    print("Running demo operations...\n\n", .{});
    print("SET(1, \"hello\")\n", .{});
    try db.set(1, "hello");

    print("SET(2, \"world\")\n", .{});
    try db.set(2, "world");

    if (try db.get(1)) |v| {
        print("GET(1) = \"{s}\"\n", .{v});
    } else {
        print("GET(1) = null\n", .{});
    }

    if (try db.get(2)) |v| {
        print("GET(2) = \"{s}\"\n", .{v});
    } else {
        print("GET(2) = null\n", .{});
    }

    print("\nDELETE(1)\n", .{});
    try db.delete(1);

    if (try db.get(1)) |v| {
        print("GET(1) = \"{s}\"\n", .{v});
    } else {
        print("GET(1) = null (expected)\n", .{});
    }

    print("\nDone. Restart the program to verify WAL replay.\n", .{});
}
