const std = @import("std");
const mem = std.mem;
const fs = std.fs;
const testing = std.testing;
const btree = @import("btree.zig");
const crc32c = std.hash.crc.Crc32Iscsi;

pub const Key = u64;
pub const Value = u64;

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

    /// Compute CRC32 of two buffers (for SET records)
    fn crc32cTwo(a: []const u8, b: []const u8) u32 {
        var h = crc32c.init();
        crc32c.update(&h, a);
        crc32c.update(&h, b);
        return crc32c.final(h);
    }

    /// Compute CRC32 of a single buffer (for HEADER and DELETE records)
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

    pub fn replay(self: *Self, allocator: mem.Allocator, index: *btree.BTree) !void {
        try self.ensureHeader();
        try self.file.seekTo(HEADER_SIZE);

        while (true) {
            const record_pos = self.file.getPos() catch 0;
            var opb: [1]u8 = undefined;
            const n = try self.file.read(opb[0..]);
            if (n == 0) break;

            const op = std.enums.fromInt(WalOp, opb[0]) orelse return WalError.InvalidWalOp;
            var kbuf: [8]u8 = undefined;
            try readExact(&self.file, kbuf[0..]);
            const key = mem.readInt(u64, &kbuf, .little);

            switch (op) {
                .set => {
                    var lbuf: [4]u8 = undefined;
                    try readExact(&self.file, &lbuf);
                    const len = mem.readInt(u32, &lbuf, .little);

                    var cbuf: [4]u8 = undefined;
                    try readExact(&self.file, &cbuf);
                    const stored_cs = mem.readInt(u32, &cbuf, .little);

                    const value_buf = try allocator.alloc(u8, len);
                    defer allocator.free(value_buf);
                    try readExact(&self.file, value_buf);

                    const calc_cs = recordChecksumSet(op, key, value_buf);
                    if (stored_cs != calc_cs) {
                        return WalError.BadChecksum;
                    }

                    try index.insert(key, record_pos);
                },
                .delete => {
                    var cbuf: [4]u8 = undefined;
                    try readExact(&self.file, &cbuf);
                    const stored_cs = mem.readInt(u32, &cbuf, .little);

                    const calc_cs = recordChecksumDelete(op, key);
                    if (stored_cs != calc_cs) {
                        return WalError.BadChecksum;
                    }
                    index.delete(key);
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
    const key: u64 = 12345;
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
    const key: u64 = 999;
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

test "WAL replay reconstructs BTree for SET records" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_test3", .{ .read = true });
    defer file.close();

    var wal = Wal.init(file);
    const pos1 = try wal.appendSet(10, "foo");
    const pos2 = try wal.appendSet(20, "bar");

    try testing.expectEqual(@as(u64, Wal.HEADER_SIZE), pos1);
    try testing.expect(pos2 > pos1);

    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var index = btree.BTree.init(gpa.allocator());
    defer index.deinit();

    try wal.replay(gpa.allocator(), &index);
    try testing.expectEqual(@as(?u64, pos1), index.search(10));
    try testing.expectEqual(@as(?u64, pos2), index.search(20));
}

test "WAL replay stops cleanly on truncated/partial record" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("wal_test5", .{ .read = true });
    defer file.close();

    var header: [Wal.HEADER_SIZE]u8 = undefined;
    header[0..4].* = "ZDB1".*;
    mem.writeInt(u32, header[4..8], 1, .little);

    const hcs = Wal.crc32cOne(header[0..8]);
    mem.writeInt(u32, header[8..12], hcs, .little);
    try file.writeAll(header[0..]);

    try file.writeAll(&[_]u8{ @intFromEnum(WalOp.set), 0xAA, 0xBB, 0xCC });

    var wal = Wal.init(file);
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var index = btree.BTree.init(gpa.allocator());
    defer index.deinit();

    const result = wal.replay(gpa.allocator(), &index);
    try testing.expectError(error.UnexpectedEndOfFile, result);
}
