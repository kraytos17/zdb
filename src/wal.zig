const std = @import("std");
const mem = std.mem;
const fs = std.fs;
const testing = std.testing;
const btree = @import("btree.zig");
const crc32c = std.hash.crc.Crc32Iscsi;

pub const Key = u64;

pub const WalOp = enum(u8) {
    set = 1,
    delete = 2,
};

pub const WalError = error{
    InvalidWalOp,
    BadHeader,
    BadChecksum,
};

pub const Wal = struct {
    file: fs.File,
    header_initialized: bool = false,

    const Self = @This();
    const MAGIC = [_]u8{ 'Z', 'D', 'B', '1' };
    const VERSION: u32 = 1;
    const HEADER_SIZE: usize = 4 + 4 + 4;

    pub fn init(file: fs.File) Self {
        return .{ .file = file };
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
    }

    fn ensureHeader(self: *Self) !void {
        if (self.header_initialized) return;

        const size = try self.file.getEndPos();
        if (size == 0) {
            var buf: [HEADER_SIZE]u8 = undefined;

            buf[0] = MAGIC[0];
            buf[1] = MAGIC[1];
            buf[2] = MAGIC[2];
            buf[3] = MAGIC[3];

            mem.writeInt(u32, buf[4..8], VERSION, .little);

            const cs = crc32cOne(buf[0..8]);
            mem.writeInt(u32, buf[8..12], cs, .little);

            try self.file.seekTo(0);
            try self.file.writeAll(buf[0..]);

            self.header_initialized = true;
        } else if (size >= HEADER_SIZE) {
            try self.file.seekTo(0);

            var buf: [HEADER_SIZE]u8 = undefined;
            try readExact(&self.file, buf[0..]);

            if (!mem.eql(u8, buf[0..4], MAGIC[0..])) {
                return WalError.BadHeader;
            }

            const version = mem.readInt(u32, buf[4..8], .little);
            if (version != VERSION) {
                return WalError.BadHeader;
            }

            const stored_cs = mem.readInt(u32, buf[8..12], .little);
            const calc_cs = crc32cOne(buf[0..8]);
            if (stored_cs != calc_cs) {
                return WalError.BadHeader;
            }

            self.header_initialized = true;
        } else {
            return WalError.BadHeader;
        }
    }

    fn crc32cTwo(a: []const u8, b: []const u8) u32 {
        var h = crc32c.init();
        crc32c.update(&h, a);
        crc32c.update(&h, b);
        return crc32c.final(h);
    }

    fn crc32cOne(a: []const u8) u32 {
        var h = crc32c.init();
        crc32c.update(&h, a);
        return crc32c.final(h);
    }

    fn recordChecksumSet(op: WalOp, key: Key, value: []const u8) u32 {
        var meta: [1 + 8 + 4]u8 = undefined;
        meta[0] = @intFromEnum(op);
        mem.writeInt(u64, meta[1..9], key, .little);
        mem.writeInt(u32, meta[9..13], @intCast(value.len), .little);
        return crc32cTwo(meta[0..], value);
    }

    fn recordChecksumDelete(op: WalOp, key: Key) u32 {
        var meta: [1 + 8]u8 = undefined;
        meta[0] = @intFromEnum(op);
        mem.writeInt(u64, meta[1..9], key, .little);
        return crc32cOne(meta[0..]);
    }

    pub fn appendSet(self: *Self, key: Key, value: []const u8) !u64 {
        try self.ensureHeader();

        const pos = try self.file.getEndPos();

        var buf: [4096]u8 = undefined;
        var writer = self.file.writer(buf[0..]);
        try writer.seekTo(pos);

        const io_w = &writer.interface;
        const op: WalOp = .set;
        const checksum = recordChecksumSet(op, key, value);

        try io_w.writeAll(&[_]u8{@intFromEnum(op)});

        var kbuf: [8]u8 = undefined;
        mem.writeInt(u64, &kbuf, key, .little);
        try io_w.writeAll(&kbuf);

        var lbuf: [4]u8 = undefined;
        mem.writeInt(u32, &lbuf, @intCast(value.len), .little);
        try io_w.writeAll(&lbuf);

        var cbuf: [4]u8 = undefined;
        mem.writeInt(u32, &cbuf, checksum, .little);
        try io_w.writeAll(&cbuf);

        try io_w.writeAll(value);
        try writer.end();

        return pos;
    }

    pub fn appendDelete(self: *Self, key: Key) !u64 {
        try self.ensureHeader();

        const pos = try self.file.getEndPos();
        var buf: [64]u8 = undefined;
        var writer = self.file.writer(buf[0..]);

        try writer.seekTo(pos);
        const io_w = &writer.interface;

        const op: WalOp = .delete;
        const checksum = recordChecksumDelete(op, key);

        try io_w.writeAll(&[_]u8{@intFromEnum(op)});

        var kbuf: [8]u8 = undefined;
        mem.writeInt(u64, &kbuf, key, .little);
        try io_w.writeAll(&kbuf);

        var cbuf: [4]u8 = undefined;
        mem.writeInt(u32, &cbuf, checksum, .little);
        try io_w.writeAll(&cbuf);

        try writer.end();
        return pos;
    }

    inline fn checkHandlerType(handler: anytype) void {
        const T = @TypeOf(handler);

        comptime {
            const info = @typeInfo(T);
            if (info != .pointer) {
                @compileError("WAL replay handler must be a *struct pointer.");
            }

            const child = info.pointer.child;
            const child_info = @typeInfo(child);
            if (child_info != .@"struct") {
                @compileError("WAL replay handler must be a pointer to a struct.");
            }

            if (!@hasDecl(child, "handleSet")) {
                @compileError(
                    \\WAL replay handler struct must declare:
                    \\    pub fn handleSet(self: *Self, key: Key, value: []const u8) !void
                );
            }
            if (!@hasDecl(child, "handleDelete")) {
                @compileError(
                    \\WAL replay handler struct must declare:
                    \\    pub fn handleDelete(self: *Self, key: Key) !void
                );
            }
        }
    }

    pub fn replayFn(self: *Self, allocator: mem.Allocator, handler: anytype) !void {
        checkHandlerType(handler);
        try self.ensureHeader();
        try self.file.seekTo(HEADER_SIZE);

        while (true) {
            var opb: [1]u8 = undefined;
            const n = try self.file.read(opb[0..]);
            if (n == 0) break; // End of WAL

            const op = std.enums.fromInt(WalOp, opb[0]) orelse return WalError.InvalidWalOp;
            var kbuf: [8]u8 = undefined;

            try readExact(&self.file, kbuf[0..]);
            const key = mem.readInt(u64, kbuf[0..], .little);
            switch (op) {
                .set => {
                    var lbuf: [4]u8 = undefined;
                    try readExact(&self.file, lbuf[0..]);
                    const len = mem.readInt(u32, lbuf[0..], .little);

                    var cbuf: [4]u8 = undefined;
                    try readExact(&self.file, cbuf[0..]);
                    const stored_cs = mem.readInt(u32, cbuf[0..], .little);

                    const value_buf = try allocator.alloc(u8, len);
                    defer allocator.free(value_buf);
                    try readExact(&self.file, value_buf);

                    const calc_cs = recordChecksumSet(op, key, value_buf);
                    if (stored_cs != calc_cs) return WalError.BadChecksum;
                    try handler.handleSet(key, value_buf);
                },

                .delete => {
                    var cbuf: [4]u8 = undefined;
                    try readExact(&self.file, cbuf[0..]);
                    const stored_cs = mem.readInt(u32, cbuf[0..], .little);
                    const calc_cs = recordChecksumDelete(op, key);

                    if (stored_cs != calc_cs) return WalError.BadChecksum;
                    try handler.handleDelete(key);
                },
            }
        }
    }

    fn readExact(file: *fs.File, buf: []u8) !void {
        var nread: usize = 0;
        while (nread < buf.len) {
            const n = try file.read(buf[nread..]);
            if (n == 0) return error.UnexpectedEndOfFile;
            nread += n;
        }
    }
};

test "WAL appendSet writes correct bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_test1", .{ .read = true });
    defer file.close();

    var wal = Wal.init(file);
    const key: Key = 12345;
    const value = "hello";

    const pos = try wal.appendSet(key, value);
    try testing.expectEqual(@as(u64, Wal.HEADER_SIZE), pos);
    try file.seekTo(0);

    var buf: [Wal.HEADER_SIZE + 1 + 8 + 4 + 4 + 5]u8 = undefined;
    const n = try file.readAll(buf[0..]);
    try testing.expectEqual(buf.len, n);

    try testing.expectEqualSlices(u8, buf[0..4], "ZDB1");

    const version = mem.readInt(u32, buf[4..8], .little);
    try testing.expectEqual(@as(u32, 1), version);

    const stored_header_cs = mem.readInt(u32, buf[8..12], .little);
    const calc_header_cs = Wal.crc32cOne(buf[0..8]);
    try testing.expectEqual(calc_header_cs, stored_header_cs);

    var off: usize = Wal.HEADER_SIZE;
    try testing.expectEqual(buf[off], @intFromEnum(WalOp.set));
    off += 1;

    const key_slice = buf[off .. off + 8];
    const read_key = mem.readInt(
        u64,
        @ptrCast(key_slice.ptr),
        .little,
    );

    try testing.expectEqual(key, read_key);
    off += 8;

    const len_slice = buf[off .. off + 4];
    const len = mem.readInt(
        u32,
        @ptrCast(len_slice.ptr),
        .little,
    );

    try testing.expectEqual(@as(u32, value.len), len);
    off += 4;

    const stored_cs_slice = buf[off .. off + 4];
    const stored_cs = mem.readInt(u32, @ptrCast(stored_cs_slice.ptr), .little);
    off += 4;

    const calc_cs = Wal.recordChecksumSet(.set, key, value);
    try testing.expectEqual(calc_cs, stored_cs);
    try testing.expectEqualSlices(u8, buf[off .. off + value.len], value);
}

test "WAL appendDelete writes correct bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_test2", .{ .read = true });
    defer file.close();

    var wal = Wal.init(file);
    const key: Key = 999;
    const pos = try wal.appendDelete(key);

    try testing.expectEqual(@as(u64, Wal.HEADER_SIZE), pos);
    try file.seekTo(0);

    var buf: [Wal.HEADER_SIZE + 1 + 8 + 4]u8 = undefined;
    const n = try file.readAll(buf[0..]);
    try testing.expectEqual(buf.len, n);

    var off: usize = Wal.HEADER_SIZE;
    try testing.expectEqual(buf[off], @intFromEnum(WalOp.delete));
    off += 1;

    const read_key_slice = buf[off .. off + 8];
    const read_key = mem.readInt(u64, @ptrCast(read_key_slice.ptr), .little);
    try testing.expectEqual(key, read_key);
    off += 8;

    const stored_cs_slice = buf[off .. off + 4];
    const stored_cs = mem.readInt(u32, @ptrCast(stored_cs_slice.ptr), .little);
    const calc_cs = Wal.recordChecksumDelete(.delete, key);
    try testing.expectEqual(calc_cs, stored_cs);
}

test "checkHandlerType allows correct handler" {
    const Handler = struct {
        pub fn handleSet(_: *@This(), _: Key, _: []const u8) !void {}
        pub fn handleDelete(_: *@This(), _: Key) !void {}
    };

    comptime {
        var h: Handler = .{};
        Wal.checkHandlerType(&h);
    }
}

test "WAL replayFn reconstructs BTree for SET records" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_replay_set", .{ .read = true });
    defer file.close();

    var wal = Wal.init(file);

    _ = try wal.appendSet(10, "foo");
    _ = try wal.appendSet(20, "hello!");

    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var index = btree.BTree.init(gpa.allocator());
    defer index.deinit();

    var set_count: usize = 0;

    const Handler = struct {
        index: *btree.BTree,
        set_count: *usize,

        const Self = @This();

        pub fn handleSet(self: *Self, key: Key, value: []const u8) !void {
            self.set_count.* += 1;
            try self.index.insert(key, @as(u64, value.len));
        }

        pub fn handleDelete(self: *Self, key: Key) !void {
            self.index.delete(key);
        }
    };

    var handler = Handler{
        .index = &index,
        .set_count = &set_count,
    };

    try wal.replayFn(gpa.allocator(), &handler);

    try testing.expectEqual(@as(?u64, 3), index.search(10));
    try testing.expectEqual(@as(?u64, 6), index.search(20));
    try testing.expectEqual(@as(usize, 2), set_count);
}

test "WAL replayFn applies DELETE records" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_replay_delete", .{ .read = true });
    defer file.close();

    var wal = Wal.init(file);
    _ = try wal.appendSet(42, "hello");
    _ = try wal.appendDelete(42);

    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var index = btree.BTree.init(gpa.allocator());
    defer index.deinit();

    var delete_count: usize = 0;

    const Handler = struct {
        index: *btree.BTree,
        delete_count: *usize,

        const Self = @This();

        pub fn handleSet(self: *Self, key: Key, value: []const u8) !void {
            try self.index.insert(key, @as(u64, value.len));
        }

        pub fn handleDelete(self: *Self, key: Key) !void {
            self.delete_count.* += 1;
            self.index.delete(key);
        }
    };

    var handler = Handler{ .index = &index, .delete_count = &delete_count };
    try wal.replayFn(gpa.allocator(), &handler);

    try testing.expectEqual(null, index.search(42));
    try testing.expectEqual(@as(usize, 1), delete_count);
}

test "WAL replayFn stops cleanly on truncated/partial record" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_replay_trunc", .{ .read = true });
    defer file.close();

    var header: [Wal.HEADER_SIZE]u8 = undefined;
    header[0..4].* = Wal.MAGIC;
    mem.writeInt(u32, header[4..8], Wal.VERSION, .little);

    const cs = Wal.crc32cOne(header[0..8]);
    mem.writeInt(u32, header[8..12], cs, .little);

    try file.writeAll(header[0..]);

    try file.writeAll(&[_]u8{
        @intFromEnum(WalOp.set),
        0xAA,
        0xBB,
        0xCC,
    });

    var wal = Wal.init(file);
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var index = btree.BTree.init(gpa.allocator());
    defer index.deinit();

    var invoked = false;

    const Handler = struct {
        index: *btree.BTree,
        invoked_flag: *bool,

        const Self = @This();

        pub fn handleSet(self: *Self, key: Key, value: []const u8) !void {
            self.invoked_flag.* = true; // MUST NEVER HAPPEN
            try self.index.insert(key, @as(u64, value.len));
        }

        pub fn handleDelete(self: *Self, _: Key) !void {
            self.invoked_flag.* = true; // MUST NEVER HAPPEN
        }
    };

    var handler = Handler{
        .index = &index,
        .invoked_flag = &invoked,
    };

    const result = wal.replayFn(gpa.allocator(), &handler);
    try testing.expectError(error.UnexpectedEndOfFile, result);
    try testing.expectEqual(false, invoked);
}
