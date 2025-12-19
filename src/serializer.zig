const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const Value = @import("parser.zig").Value;

pub const Serializer = struct {
    /// Format: [NumColumns: u32] [Col1 Type: u8] [Col1 Data...] [Col2 Type: u8] [Col2 Data...]
    pub fn serialize(allocator: Allocator, values: []const Value) ![]u8 {
        var total_size: usize = 4;
        for (values) |val| {
            switch (val) {
                .integer => {
                    total_size += 1 + 8;
                },
                .text => |str| {
                    total_size += 1 + 4 + str.len;
                },
            }
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);

        var pos: usize = 0;
        mem.writeInt(u32, buf[pos..][0..4], @intCast(values.len), .little);
        pos += 4;

        for (values) |val| {
            switch (val) {
                .integer => |num| {
                    buf[pos] = 1;
                    pos += 1;
                    mem.writeInt(i64, buf[pos..][0..8], num, .little);
                    pos += 8;
                },
                .text => |str| {
                    buf[pos] = 2;
                    pos += 1;
                    mem.writeInt(u32, buf[pos..][0..4], @intCast(str.len), .little);
                    pos += 4;
                    @memcpy(buf[pos..][0..str.len], str);
                    pos += str.len;
                },
            }
        }
        return buf;
    }

    pub fn deserialize(allocator: Allocator, data: []const u8) ![]Value {
        var pos: usize = 0;

        if (data.len < 4) return error.CorruptData;
        const num_cols = mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        var values = try std.ArrayList(Value).initCapacity(allocator, num_cols);
        errdefer {
            for (values.items) |v| {
                switch (v) {
                    .text => |s| allocator.free(s),
                    else => {},
                }
            }
            values.deinit(allocator);
        }

        var i: usize = 0;
        while (i < num_cols) : (i += 1) {
            if (pos >= data.len) return error.CorruptData;

            const type_id = data[pos];
            pos += 1;
            switch (type_id) {
                1 => {
                    if (pos + 8 > data.len) return error.CorruptData;

                    const num = mem.readInt(i64, data[pos..][0..8], .little);
                    pos += 8;

                    try values.append(allocator, Value{ .integer = num });
                },
                2 => {
                    if (pos + 4 > data.len) return error.CorruptData;

                    const len = mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;

                    if (pos + len > data.len) return error.CorruptData;

                    const str_buf = try allocator.alloc(u8, len);
                    @memcpy(str_buf, data[pos..][0..len]);
                    pos += len;

                    try values.append(allocator, Value{ .text = str_buf });
                },
                else => return error.CorruptData,
            }
        }
        return values.toOwnedSlice(allocator);
    }
};

test "Serializer roundtrip" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const input_row = [_]Value{
        .{ .integer = 42 },
        .{ .text = "Hello Zig Database" },
        .{ .integer = -999 },
    };

    const bytes = try Serializer.serialize(alloc, &input_row);
    defer alloc.free(bytes);

    const output_row = try Serializer.deserialize(alloc, bytes);
    defer {
        for (output_row) |v| {
            switch (v) {
                .text => |s| alloc.free(s),
                else => {},
            }
        }
        alloc.free(output_row);
    }

    try testing.expectEqual(3, output_row.len);
    try testing.expectEqual(42, output_row[0].integer);
    try testing.expectEqualStrings("Hello Zig Database", output_row[1].text);
    try testing.expectEqual(-999, output_row[2].integer);
}
