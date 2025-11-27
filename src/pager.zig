const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const testing = std.testing;

const page = @import("page.zig");
const wal = @import("wal.zig");

const Page = page.Page;
const Wal = wal.Wal;
const PAGE_SIZE = page.PAGE_SIZE;

pub const Pager = struct {
    pub const CacheEntry = struct {
        page: Page,
        id: u32,
        is_dirty: bool = false,
        ref_cnt: usize = 0,
        next_dirty: ?*CacheEntry = null,
    };

    file: fs.File,
    allocator: mem.Allocator,
    wal: *Wal,
    cache: std.AutoHashMap(u32, *CacheEntry),
    dirty_head: ?*CacheEntry = null,

    const Self = @This();

    pub fn open(dir: fs.Dir, path: []const u8, allocator: mem.Allocator) !Self {
        const file = dir.openFile(path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try dir.createFile(path, .{ .read = true }),
            else => return err,
        };
        errdefer file.close();

        const wal_path = try std.fmt.allocPrint(allocator, "{s}.wal", .{path});
        defer allocator.free(wal_path);

        const wal_file = dir.openFile(wal_path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try dir.createFile(wal_path, .{ .read = true }),
            else => return err,
        };
        errdefer wal_file.close();

        const wal_ptr = try allocator.create(Wal);
        errdefer allocator.destroy(wal_ptr);

        wal_ptr.* = Wal.init(wal_file);

        return .{
            .file = file,
            .allocator = allocator,
            .wal = wal_ptr,
            .cache = .init(allocator),
            .dirty_head = null,
        };
    }

    pub fn close(self: *Self) !void {
        self.flush() catch {};
        var it = self.cache.valueIterator();
        while (it.next()) |entry_ptr| {
            self.allocator.free(entry_ptr.*.page.buf);
            self.allocator.destroy(entry_ptr.*);
        }

        self.cache.deinit();
        self.wal.deinit();
        self.allocator.destroy(self.wal);
        self.file.close();
    }

    pub fn get(self: *Self, page_id: u32) !*CacheEntry {
        if (self.cache.get(page_id)) |entry| {
            entry.ref_cnt += 1;
            return entry;
        }

        const buf = try self.allocator.alloc(u8, PAGE_SIZE);
        errdefer self.allocator.free(buf);

        const entry = try self.allocator.create(CacheEntry);
        errdefer self.allocator.destroy(entry);

        const offset = @as(u64, page_id) * PAGE_SIZE;
        try self.file.seekTo(offset);
        const bytes_read = try self.file.readAll(buf);

        if (bytes_read == 0) {
            entry.page = .init(buf);
        } else {
            if (bytes_read < PAGE_SIZE) @memset(buf[bytes_read..], 0);
            entry.page = .{ .buf = buf };
        }

        entry.id = page_id;
        entry.is_dirty = false;
        entry.ref_cnt = 1;
        entry.next_dirty = null;

        try self.cache.put(page_id, entry);
        return entry;
    }

    pub fn unpin(_: *Self, entry: *CacheEntry) void {
        std.debug.assert(entry.ref_cnt > 0);
        entry.ref_cnt -= 1;
    }

    pub fn makeDirty(self: *Self, entry: *CacheEntry) void {
        if (entry.is_dirty) return;
        entry.is_dirty = true;
        entry.next_dirty = self.dirty_head;
        self.dirty_head = entry;
    }

    pub fn flush(self: *Self) !void {
        while (self.dirty_head) |entry| {
            const offset = @as(u64, entry.id) * PAGE_SIZE;
            try self.file.seekTo(offset);
            try self.file.writeAll(entry.page.buf);

            const next = entry.next_dirty;
            entry.is_dirty = false;
            entry.next_dirty = null;
            self.dirty_head = next;
        }
        try self.file.sync();
    }
};

test "Pager: Basic Persistence (Write -> Close -> Read)" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = "test_persistence.db";
    {
        var pager = try Pager.open(tmp.dir, path, allocator);
        defer pager.close() catch {};

        const entry = try pager.get(0);
        _ = try entry.page.insert("Hello Zig Database");

        pager.makeDirty(entry);
        pager.unpin(entry);
        try pager.flush();
    }
    {
        var pager = try Pager.open(tmp.dir, path, allocator);
        defer pager.close() catch {};

        const entry = try pager.get(0);
        defer pager.unpin(entry);

        const val = entry.page.get(0).?;
        try testing.expectEqualSlices(u8, "Hello Zig Database", val);
    }
}

test "Pager: Cache Identity & Reference Counting" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = "test_cache.db";

    var pager = try Pager.open(tmp.dir, path, allocator);
    defer pager.close() catch {};

    const p1 = try pager.get(1);
    const p2 = try pager.get(1);

    try testing.expect(p1 == p2);
    try testing.expectEqual(@as(usize, 2), p1.ref_cnt);

    pager.unpin(p1);
    try testing.expectEqual(@as(usize, 1), p2.ref_cnt);

    pager.unpin(p2);
    try testing.expectEqual(@as(usize, 0), p2.ref_cnt);
}

test "Pager: Dirty List Logic" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = "test_dirty.db";

    var pager = try Pager.open(tmp.dir, path, allocator);
    defer pager.close() catch {};

    const p0 = try pager.get(0);
    const p1 = try pager.get(1);
    defer pager.unpin(p0);
    defer pager.unpin(p1);

    try testing.expect(pager.dirty_head == null);

    pager.makeDirty(p0);
    try testing.expect(pager.dirty_head == p0);
    try testing.expect(p0.is_dirty == true);
    try testing.expect(p0.next_dirty == null);

    pager.makeDirty(p1);
    try testing.expect(pager.dirty_head == p1);
    try testing.expect(p1.next_dirty == p0);

    pager.makeDirty(p0);
    try testing.expect(pager.dirty_head == p1);
    try testing.expect(p0.next_dirty == null);

    try pager.flush();

    try testing.expect(pager.dirty_head == null);
    try testing.expect(p0.is_dirty == false);
    try testing.expect(p1.is_dirty == false);
    try testing.expect(p0.next_dirty == null);
    try testing.expect(p1.next_dirty == null);
}
