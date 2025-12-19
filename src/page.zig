const std = @import("std");
const mem = std.mem;
const testing = std.testing;

pub const PAGE_SIZE: usize = 4096;
const TOMBSTONE: u16 = 0xFFFF;
const HEADER_SIZE: u16 = 6;

pub const Page = struct {
    buf: []u8,

    const Self = @This();

    pub fn init(buf: []u8) Self {
        // Header:
        // [0..2] = numRecords
        // [2..4] = freeStart
        // [4..6] = freeEnd
        mem.writeInt(u16, buf[0..2], 0, .little);
        mem.writeInt(u16, buf[2..4], HEADER_SIZE, .little);
        mem.writeInt(u16, buf[4..6], PAGE_SIZE, .little);
        return .{ .buf = buf };
    }

    fn readU16(self: *const Self, off: u16) u16 {
        return mem.readInt(
            u16,
            @ptrCast(self.buf[off .. off + 2].ptr),
            .little,
        );
    }

    fn writeU16(self: *Self, off: u16, v: u16) void {
        mem.writeInt(
            u16,
            @ptrCast(self.buf[off .. off + 2].ptr),
            v,
            .little,
        );
    }

    fn numRecords(self: *const Self) u16 {
        return self.readU16(0);
    }

    fn freeStart(self: *const Self) u16 {
        return self.readU16(2);
    }

    fn freeEnd(self: *const Self) u16 {
        return self.readU16(4);
    }

    fn setNumRecords(self: *Self, n: u16) void {
        self.writeU16(0, n);
    }

    fn setFreeStart(self: *Self, v: u16) void {
        self.writeU16(2, v);
    }

    fn setFreeEnd(self: *Self, v: u16) void {
        self.writeU16(4, v);
    }

    fn slotOffset(idx: u16) u16 {
        // Slots grow downward from end of page.
        return @intCast(PAGE_SIZE - 2 * (idx + 1));
    }

    fn getSlot(self: *const Self, idx: u16) u16 {
        return self.readU16(slotOffset(idx));
    }

    fn setSlot(self: *Self, idx: u16, value: u16) void {
        self.writeU16(slotOffset(idx), value);
    }

    fn freeSpace(self: *const Self) u16 {
        const fs = self.freeStart();
        const fe = self.freeEnd();
        return if (fe > fs) fe - fs else 0;
    }

    pub fn canInsert(self: *const Self, payload_len: u16) bool {
        // record format: [len: u16][payload], plus 1 slot entry (2 bytes)
        const need = 2 + payload_len + 2;
        return self.freeSpace() >= need;
    }

    pub fn insert(self: *Self, payload: []const u8) !u16 {
        const num = self.numRecords();
        const len: u16 = @intCast(payload.len);
        const total_len = 2 + len;

        if (!self.canInsert(len)) return error.OutOfSpace;
        const rec_off = self.freeStart();
        self.writeU16(rec_off, len);
        @memcpy(self.buf[rec_off + 2 .. rec_off + 2 + payload.len], payload);

        self.setFreeStart(rec_off + total_len);
        const new_slot_off = self.freeEnd() - 2;
        self.setFreeEnd(new_slot_off);
        self.writeU16(new_slot_off, rec_off);

        self.setNumRecords(num + 1);
        return num;
    }

    pub fn get(self: *const Self, idx: u16) ?[]const u8 {
        if (idx >= self.numRecords()) return null;

        const rec_off = self.getSlot(idx);
        if (rec_off == TOMBSTONE) return null;

        const len = self.readU16(rec_off);
        return self.buf[rec_off + 2 .. rec_off + 2 + len];
    }

    pub fn delete(self: *Self, idx: u16) !void {
        if (idx >= self.numRecords()) return error.OutOfBounds;
        self.setSlot(idx, TOMBSTONE);
    }

    pub fn defragment(self: *Self) void {
        var new_free_start: u16 = HEADER_SIZE;
        var new_free_end: u16 = PAGE_SIZE;
        var new_idx: u16 = 0;

        const num = self.numRecords();
        var i: u16 = 0;
        while (i < num) : (i += 1) {
            const old_off = self.getSlot(i);
            if (old_off == TOMBSTONE) continue;

            const len = self.readU16(old_off);
            const total_len = 2 + len;
            @memmove(
                self.buf[new_free_start .. new_free_start + total_len],
                self.buf[old_off .. old_off + total_len],
            );

            new_free_end -= 2;
            self.writeU16(new_free_end, new_free_start);

            new_free_start += total_len;
            new_idx += 1;
        }

        self.setNumRecords(new_idx);
        self.setFreeStart(new_free_start);
        self.setFreeEnd(new_free_end);
    }
};

test "Page init sets correct header values" {
    var buf: [PAGE_SIZE]u8 = undefined;

    var p = Page.init(buf[0..]);

    try testing.expectEqual(@as(u16, 0), p.numRecords());
    try testing.expectEqual(@as(u16, 6), p.freeStart());
    try testing.expectEqual(@as(u16, PAGE_SIZE), p.freeEnd());
}

test "Insert a single record and read it back" {
    var buf: [PAGE_SIZE]u8 = undefined;
    var p = Page.init(buf[0..]);

    const rec = "hello";

    const idx = try p.insert(rec);
    try testing.expectEqual(@as(u16, 0), idx);

    const out = p.get(0) orelse return error.TestFailed;
    try testing.expectEqualSlices(u8, rec, out);
}

test "Insert multiple records and read them back" {
    var buf: [PAGE_SIZE]u8 = undefined;
    var p = Page.init(buf[0..]);

    const a = "foo";
    const b = "barbaz";
    const c = "ziglang";

    const ia = try p.insert(a);
    const ib = try p.insert(b);
    const ic = try p.insert(c);

    try testing.expectEqual(@as(u16, 0), ia);
    try testing.expectEqual(@as(u16, 1), ib);
    try testing.expectEqual(@as(u16, 2), ic);

    try testing.expectEqualSlices(u8, a, p.get(0).?);
    try testing.expectEqualSlices(u8, b, p.get(1).?);
    try testing.expectEqualSlices(u8, c, p.get(2).?);
}

test "Insert until full, then fail with OutOfSpace" {
    var buf: [PAGE_SIZE]u8 = undefined;
    var p = Page.init(buf[0..]);

    const rec = [_]u8{1} ** 100;
    while (p.canInsert(rec.len)) {
        _ = try p.insert(rec[0..]);
    }
    try testing.expectError(error.OutOfSpace, p.insert(rec[0..]));
}

test "Delete creates tombstone but record still exists prior to defrag" {
    var buf: [PAGE_SIZE]u8 = undefined;
    var p = Page.init(buf[0..]);

    const a = "hello";
    const b = "world";

    const ia = try p.insert(a);
    const ib = try p.insert(b);

    try testing.expectEqual(@as(u16, 0), ia);
    try testing.expectEqual(@as(u16, 1), ib);

    try p.delete(ia);

    const slot_off = Page.slotOffset(ia);
    const slot_val = mem.readInt(u16, @ptrCast(buf[slot_off .. slot_off + 2].ptr), .little);
    try testing.expectEqual(@as(u16, 0xFFFF), slot_val);

    try testing.expectEqualSlices(u8, b, p.get(1).?);
    try testing.expectEqual(@as(?[]const u8, null), p.get(0));
}

test "Defragment removes tombstones and compacts records" {
    var buf: [PAGE_SIZE]u8 = undefined;
    var p = Page.init(buf[0..]);

    const a = "a";
    const b = "bbbbb";
    const c = "ccc";

    _ = try p.insert(a);
    const ib = try p.insert(b);
    _ = try p.insert(c);

    try p.delete(ib);
    p.defragment();

    try testing.expectEqual(@as(u16, 2), p.numRecords());
    try testing.expectEqualSlices(u8, a, p.get(0).?);
    try testing.expectEqualSlices(u8, c, p.get(1).?);
    try testing.expectEqual(@as(u16, 6 + (2 + a.len + 2 + c.len)), p.freeStart());
}

test "Defragment moves records to front of page" {
    var buf: [PAGE_SIZE]u8 = undefined;
    var p = Page.init(buf[0..]);

    const x = "11111";
    const y = "222222";
    const z = "333";

    _ = try p.insert(x);
    const iy = try p.insert(y);
    _ = try p.insert(z);

    try p.delete(iy);
    p.defragment();

    const off_x = p.getSlot(0);
    const off_z = p.getSlot(1);

    try testing.expectEqual(@as(u16, 6), off_x);
    try testing.expectEqual(@as(u16, 6 + (2 + x.len)), off_z);
}
