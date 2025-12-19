const std = @import("std");
const fs = std.fs;

const Db = @import("db.zig").Db;
const ParseError = @import("parser.zig").ParseError;
const Parser = @import("parser.zig").Parser;
const VM = @import("vm.zig").Vm;

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer {
        const check = gpa.deinit();
        if (check == .leak) std.debug.print("\n[!] Memory Leak Detected!\n", .{});
    }

    const allocator = gpa.allocator();
    const cwd = std.fs.cwd();

    const stdin_buffer = try allocator.alloc(u8, 1024);
    defer allocator.free(stdin_buffer);

    const stdout_buffer = try allocator.alloc(u8, 1024);
    defer allocator.free(stdout_buffer);

    const stdin_reader = try allocator.create(fs.File.Reader);
    const stdout_writer = try allocator.create(fs.File.Writer);
    defer allocator.destroy(stdin_reader);
    defer allocator.destroy(stdout_writer);

    stdin_reader.* = fs.File.readerStreaming(fs.File.stdin(), stdin_buffer);
    stdout_writer.* = fs.File.writerStreaming(fs.File.stdout(), stdout_buffer);

    const stdin = &stdin_reader.interface;
    const stdout = &stdout_writer.interface;

    try stdout.print("zdb: Opening 'demo.db'...\n", .{});
    try stdout.flush();

    var db = try Db.open(cwd, "demo.db", allocator);
    defer db.close() catch {};

    var vm = VM.init(allocator, &db);

    try stdout.print("zdb: Ready.\n", .{});
    try stdout.print("     Try: insert into users values (1, 'Alice', 'admin');\n", .{});
    try stdout.print("     Try: select * from users where id = 1;\n", .{});
    try stdout.print("     Type '.exit' to quit.\n\n", .{});
    try stdout.flush();

    while (true) {
        try stdout.print("zdb > ", .{});
        try stdout.flush();

        const user_input = stdin.takeDelimiterInclusive('\n') catch |err| {
            if (err == error.EndOfStream) break;
            try stdout.print("Input Error: {}\n", .{err});
            try stdout.flush();
            break;
        };

        const query = std.mem.trim(u8, user_input, &std.ascii.whitespace);
        if (query.len == 0) continue;

        if (std.mem.eql(u8, query, ".exit")) break;
        handleQuery(&vm, allocator, query) catch |err| {
            switch (err) {
                ParseError.UnexpectedToken => try stdout.print("Error: Syntax error (Unexpected Token)\n", .{}),
                ParseError.InvalidSyntax => try stdout.print("Error: Invalid SQL syntax\n", .{}),
                ParseError.IntegerOverflow => try stdout.print("Error: Integer too large\n", .{}),
                error.ColumnNotFound => try stdout.print("Error: Unknown column in WHERE clause\n", .{}),
                else => try stdout.print("Error: {any}\n", .{err}),
            }
        };
        try stdout.flush();
    }
}

fn handleQuery(vm: *VM, allocator: std.mem.Allocator, sql: []const u8) !void {
    var parser = Parser.init(allocator, sql);
    const stmt = try parser.parse();

    defer {
        switch (stmt) {
            .INSERT => |ins| allocator.free(ins.values),
            .SELECT => |sel| {
                if (sel.where) |expr| {
                    freeExpression(allocator, expr);
                }
            },
        }
    }
    try vm.execute(stmt);
}

fn freeExpression(allocator: std.mem.Allocator, expr: *const @import("parser.zig").Expression) void {
    switch (expr.*) {
        .Binary => |bin| {
            freeExpression(allocator, bin.left);
            freeExpression(allocator, bin.right);
            allocator.destroy(expr);
        },
        else => allocator.destroy(expr),
    }
}
