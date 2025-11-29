const std = @import("std");

pub fn main() !void {
    const cwd = std.fs.cwd();

    var file = try cwd.openFile("demo.db", .{});
    defer file.close();

    const PAGE_SIZE = 4096;
    var buf: [PAGE_SIZE]u8 = undefined;

    const n = try file.readAll(&buf);
    std.debug.print("Read {d} bytes from demo.db\n", .{n});
    if (n < 6) {
        std.debug.print("File too small to contain a page header.\n", .{});
        return;
    }

    const rec_count = std.mem.readInt(u16, buf[0..2], .little);
    const free_start = std.mem.readInt(u16, buf[2..4], .little);
    const free_end = std.mem.readInt(u16, buf[4..6], .little);

    std.debug.print("Page 0 header:\n", .{});
    std.debug.print("  record_count = {d}\n", .{rec_count});
    std.debug.print("  free_start   = {d}\n", .{free_start});
    std.debug.print("  free_end     = {d}\n", .{free_end});

    std.debug.print("\nHex dump (first 128 bytes):\n", .{});

    var i: usize = 0;
    while (i < 128 and i < n) : (i += 1) {
        if (i % 16 == 0)
            std.debug.print("\n{d:0>4}: ", .{i});
        std.debug.print("{X:0>2} ", .{buf[i]});
    }
    std.debug.print("\n\nDone.\n", .{});
}
