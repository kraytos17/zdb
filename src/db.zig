const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const testing = std.testing;

const Pager = @import("pager.zig").Pager;
const BTree = @import("btree.zig").BTree;
const Key = @import("btree.zig").Key;
const Wal = @import("wal.zig").Wal;

pub const DbError = fs.File.ReadError || fs.File.WriteError || mem.Allocator.Error;

pub const Db = struct {
    allocator: mem.Allocator,
    pager: Pager,
    index: BTree,

    const Self = @This();

    pub fn open(dir: fs.Dir, path: []const u8, allocator: mem.Allocator) !Self {
        var pager = try Pager.open(dir, path, allocator);
        errdefer pager.close() catch {};

        const idx = BTree.init(allocator);
        var db = Self{
            .allocator = allocator,
            .pager = pager,
            .index = idx,
        };

        try db.replayWal();
        return db;
    }

    pub fn close(self: *Self) !void {
        self.index.deinit();
        try self.pager.close();
    }

    fn replayWal(self: *Self) !void {
        const Handler = struct {
            db: *Self,

            const HandlerCtx = @This();
            pub fn handleSet(ctx: *HandlerCtx, key: Key, value: []const u8) !void {
                try ctx.db.index.insert(key, value);
            }

            pub fn handleDelete(ctx: *HandlerCtx, key: Key) !void {
                ctx.db.index.delete(key);
            }
        };

        var handler = Handler{ .db = self };
        var wal = self.pager.getWal();
        try wal.replayFn(self.allocator, &handler);
    }

    pub fn set(self: *Self, key: Key, value: []const u8) !void {
        _ = try self.pager.getWal().appendSet(key, value);
        try self.index.insert(key, value);
    }

    pub fn get(self: *Self, key: Key) !?[]const u8 {
        return self.index.search(key);
    }

    pub fn delete(self: *Self, key: Key) !void {
        _ = try self.pager.getWal().appendDelete(key);
        self.index.delete(key);
    }
};

test "Db basic set/get/delete" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var db = try Db.open(tmp.dir, "mini.db", gpa.allocator());
    defer db.close() catch {};

    try db.set(10, "hello");
    try db.set(20, "world");

    const v1 = try db.get(10);
    try testing.expect(v1 != null);
    try testing.expectEqualSlices(u8, "hello", v1.?);

    const v2 = try db.get(20);
    try testing.expectEqualSlices(u8, "world", v2.?);

    try db.delete(10);
    try testing.expectEqual(@as(?[]const u8, null), try db.get(10));
}

test "Db persists across reopen via WAL replay" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const path = "mini_persist.db";
    {
        var db = try Db.open(tmp.dir, path, alloc);
        defer db.close() catch {};

        try db.set(1, "alpha");
        try db.set(2, "beta");
        try db.set(3, "gamma");
        //delete '2' to ensure deletions replay correctly too
        try db.delete(2);
    }
    // 2. Re-Open, Verify State
    {
        var db = try Db.open(tmp.dir, path, alloc);
        defer db.close() catch {};

        const v1 = try db.get(1);
        try testing.expect(v1 != null);
        try testing.expectEqualSlices(u8, "alpha", v1.?);

        const v2 = try db.get(2);
        try testing.expect(v2 == null);

        const v3 = try db.get(3);
        try testing.expect(v3 != null);
        try testing.expectEqualSlices(u8, "gamma", v3.?);
    }
}
