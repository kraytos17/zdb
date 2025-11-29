const std = @import("std");
const crc32c = std.hash.crc.Crc32Iscsi;

pub fn main() !void {
    const cwd = std.fs.cwd();

    var file = try cwd.openFile("demo.db.wal", .{});
    defer file.close();

    var header: [12]u8 = undefined;
    const n = try file.readAll(&header);
    if (n < 12) {
        std.debug.print("WAL too small.\n", .{});
        return;
    }

    std.debug.print("WAL HEADER\n", .{});
    const magic = header[0..4];
    std.debug.print("  Magic: {s}\n", .{magic});

    const version = std.mem.readInt(u32, header[4..8], .little);
    std.debug.print("  Version: {d}\n", .{version});

    const stored_cs = std.mem.readInt(u32, header[8..12], .little);
    const calc_cs = calcCrc32(header[0..8]);
    std.debug.print("  Header CRC stored={X:0>8}, calc={X:0>8}\n", .{ stored_cs, calc_cs });

    if (stored_cs != calc_cs) {
        std.debug.print("  !!!! Bad header checksum !!!!\n", .{});
    }

    std.debug.print("\nWAL RECORDS:\n", .{});

    try file.seekTo(12);
    var buf: [8]u8 = undefined;
    var index: usize = 0;
    while (true) {
        var opb: [1]u8 = undefined;
        const n1 = try file.read(opb[0..]);
        if (n1 == 0) break;

        const op = opb[0];
        try readExact(&file, buf[0..8]);
        const key = std.mem.readInt(u64, buf[0..8], .little);

        switch (op) {
            1 => {
                var lbuf: [4]u8 = undefined;
                try readExact(&file, lbuf[0..]);
                const len = std.mem.readInt(u32, lbuf[0..], .little);

                var cbuf: [4]u8 = undefined;
                try readExact(&file, cbuf[0..]);
                const stored = std.mem.readInt(u32, cbuf[0..], .little);

                const value_buf = try std.heap.page_allocator.alloc(u8, len);
                defer std.heap.page_allocator.free(value_buf);
                try readExact(&file, value_buf);

                const calc = crcRecordSet(key, value_buf);

                std.debug.print(
                    "Record #{d}: SET key={d} len={d} crc_ok={}\n",
                    .{ index, key, len, stored == calc },
                );
                std.debug.print("  value=\"{s}\"\n", .{value_buf});
                index += 1;
            },

            2 => |_| {
                var cbuf: [4]u8 = undefined;
                try readExact(&file, cbuf[0..]);
                const stored = std.mem.readInt(u32, cbuf[0..], .little);
                const calc = crcRecordDelete(key);

                std.debug.print(
                    "Record #{d}: DELETE key={d} crc_ok={}\n",
                    .{ index, key, stored == calc },
                );
                index += 1;
            },

            else => {
                std.debug.print("Unknown op {d}, stopping.\n", .{op});
                break;
            },
        }
    }

    std.debug.print("\nDone.\n", .{});
}

fn readExact(file: *std.fs.File, buf: []u8) !void {
    var nread: usize = 0;
    while (nread < buf.len) {
        const n = try file.read(buf[nread..]);
        if (n == 0) return error.UnexpectedEndOfFile;
        nread += n;
    }
}

fn calcCrc32(data: []const u8) u32 {
    var h = crc32c.init();
    crc32c.update(&h, data);
    return crc32c.final(h);
}

fn crcRecordSet(key: u64, value: []const u8) u32 {
    var meta: [1 + 8 + 4]u8 = undefined;
    meta[0] = 1;
    std.mem.writeInt(u64, meta[1..9], key, .little);
    std.mem.writeInt(u32, meta[9..13], @intCast(value.len), .little);

    var h = crc32c.init();
    crc32c.update(&h, meta[0..]);
    crc32c.update(&h, value);
    return crc32c.final(h);
}

fn crcRecordDelete(key: u64) u32 {
    var meta: [1 + 8]u8 = undefined;
    meta[0] = 2;
    std.mem.writeInt(u64, meta[1..9], key, .little);

    var h = crc32c.init();
    crc32c.update(&h, meta[0..]);
    return crc32c.final(h);
}
