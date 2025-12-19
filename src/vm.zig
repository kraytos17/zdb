const std = @import("std");

const Db = @import("db.zig").Db;
const Expression = @import("parser.zig").Expression;
const Operator = @import("parser.zig").Operator;
const Serializer = @import("serializer.zig").Serializer;
const Statement = @import("parser.zig").Statement;
const Value = @import("parser.zig").Value;

pub const Vm = struct {
    db: *Db,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db: *Db) Self {
        return .{
            .db = db,
            .allocator = allocator,
        };
    }

    pub fn execute(self: *Self, stmt: Statement) !void {
        switch (stmt) {
            .INSERT => |ins| try self.execInsert(ins),
            .SELECT => |sel| try self.execSelect(sel),
        }
    }

    fn execInsert(self: *Self, ins: anytype) !void {
        if (ins.values.len == 0) {
            std.debug.print("Error: INSERT must provide at least one column (ID).\n", .{});
            return;
        }

        const pk_val = ins.values[0];
        const pk: u64 = switch (pk_val) {
            .integer => |n| @as(u64, @intCast(n)),
            .text => {
                std.debug.print("Error: Primary Key (first column) must be an integer.\n", .{});
                return;
            },
        };

        const raw_bytes = try Serializer.serialize(self.allocator, ins.values);
        defer self.allocator.free(raw_bytes);

        try self.db.set(pk, raw_bytes);
        std.debug.print("Inserted 1 row. (Key: {d})\n", .{pk});
    }

    fn execSelect(self: *Self, sel: anytype) !void {
        var cursor = self.db.index.cursorFirst();
        var count: usize = 0;
        while (cursor.isValid()) {
            const key = cursor.key();
            const raw_data = cursor.value();
            const row = try Serializer.deserialize(self.allocator, raw_data);
            defer {
                for (row) |v| {
                    switch (v) {
                        .text => |s| self.allocator.free(s),
                        else => {},
                    }
                }
                self.allocator.free(row);
            }

            var match = true;
            if (sel.where) |expr_ptr| {
                match = try self.evaluate(expr_ptr, row);
            }
            if (match) {
                printRow(key, row);
                count += 1;
            }
            self.db.index.cursorNext(&cursor);
        }
        std.debug.print("Selected {d} rows.\n", .{count});
    }

    fn evaluate(self: *Self, expr: *Expression, row: []const Value) !bool {
        switch (expr.*) {
            .Binary => |bin| {
                const left = try resolveVal(bin.left, row);
                const right = try resolveVal(bin.right, row);

                if (left == .integer and right == .integer) {
                    const l = left.integer;
                    const r = right.integer;
                    return switch (bin.op) {
                        .Equals => l == r,
                        .NotEquals => l != r,
                        .GreaterThan => l > r,
                        .LessThan => l < r,
                        .GTE => l >= r,
                        .LTE => l <= r,
                        .And => (try self.evaluate(bin.left, row)) and (try self.evaluate(bin.right, row)),
                        .Or => (try self.evaluate(bin.left, row)) or (try self.evaluate(bin.right, row)),
                    };
                }
                if (left == .text and right == .text) {
                    const l = left.text;
                    const r = right.text;
                    return switch (bin.op) {
                        .Equals => std.mem.eql(u8, l, r),
                        .NotEquals => !std.mem.eql(u8, l, r),
                        else => false,
                    };
                }
                return false;
            },
            else => return false,
        }
    }
};

fn resolveVal(expr: *Expression, row: []const Value) !Value {
    switch (expr.*) {
        .Literal => |v| return v,
        .Column => |name| {
            // HARDCODED MAPPING (for now)
            // Col 0 = id
            // Col 1 = name
            // Col 2 = email
            if (std.mem.eql(u8, name, "id")) return getCol(row, 0);
            if (std.mem.eql(u8, name, "name")) return getCol(row, 1);
            if (std.mem.eql(u8, name, "email")) return getCol(row, 2);
            return error.ColumnNotFound;
        },
        .Binary => return error.NestedExpressionResultNotSupported,
    }
}

fn getCol(row: []const Value, idx: usize) !Value {
    if (idx >= row.len) return error.IndexOutOfBounds;
    return row[idx];
}

fn printRow(key: u64, row: []const Value) void {
    std.debug.print("Row [{d}]: (", .{key});
    for (row, 0..) |col, i| {
        switch (col) {
            .integer => |n| std.debug.print("{d}", .{n}),
            .text => |s| std.debug.print("'{s}'", .{s}),
        }
        if (i < row.len - 1) std.debug.print(", ", .{});
    }
    std.debug.print(")\n", .{});
}
