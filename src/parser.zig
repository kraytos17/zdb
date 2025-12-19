const std = @import("std");
const testing = std.testing;
const Tokenizer = @import("tokenizer.zig").Tokenizer;
const Token = @import("token.zig").Token;
const TokenType = @import("token.zig").TokenType;

const Allocator = std.mem.Allocator;

pub const Value = union(enum) {
    integer: i64,
    text: []const u8,
};

pub const Operator = enum {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GTE,
    LTE,
    And,
    Or,
};

pub const Expression = union(enum) {
    Literal: Value,
    Column: []const u8,
    Binary: struct {
        left: *Expression,
        op: Operator,
        right: *Expression,
    },
};

pub const StatementType = enum {
    INSERT,
    SELECT,
};

pub const Statement = union(StatementType) {
    INSERT: struct {
        table_name: []const u8,
        values: []const Value,
    },
    SELECT: struct {
        table_name: []const u8,
        where: ?*Expression,
    },
};

pub const ParseError = error{
    UnexpectedToken,
    InvalidSyntax,
    IntegerOverflow,
    OutOfMemory,
};

pub const Parser = struct {
    tokenizer: Tokenizer,
    curr_tok: Token,
    peek_tok: Token,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, source: []const u8) Self {
        var tokenizer = Tokenizer.init(source);
        const curr = tokenizer.next();
        const peek = tokenizer.next();

        return .{
            .tokenizer = tokenizer,
            .curr_tok = curr,
            .peek_tok = peek,
            .allocator = allocator,
        };
    }

    fn nextToken(self: *Self) void {
        self.curr_tok = self.peek_tok;
        self.peek_tok = self.tokenizer.next();
    }

    fn expectPeek(self: *Self, expected: TokenType) !void {
        if (self.peek_tok.type == expected) {
            self.nextToken();
        } else {
            return ParseError.UnexpectedToken;
        }
    }

    pub fn parse(self: *Self) !Statement {
        switch (self.curr_tok.type) {
            .KEYWORD_INSERT => return self.parseInsert(),
            .KEYWORD_SELECT => return self.parseSelect(),
            else => return ParseError.InvalidSyntax,
        }
    }

    fn parseInsert(self: *Self) !Statement {
        try self.expectPeek(.KEYWORD_INTO);
        try self.expectPeek(.IDENTIFIER);
        const table_name = self.curr_tok.lexeme;

        try self.expectPeek(.KEYWORD_VALUES);
        try self.expectPeek(.SYMBOL_LPAREN);
        var val_list = try std.ArrayList(Value).initCapacity(self.allocator, 0);
        errdefer val_list.deinit(self.allocator);

        while (true) {
            if (self.peek_tok.type == .LITERAL_NUMBER) {
                self.nextToken();
                const val = try std.fmt.parseInt(i64, self.curr_tok.lexeme, 10);
                try val_list.append(self.allocator, .{ .integer = val });
            } else if (self.peek_tok.type == .LITERAL_STRING) {
                self.nextToken();
                try val_list.append(self.allocator, .{ .text = self.curr_tok.lexeme });
            } else {
                return ParseError.UnexpectedToken;
            }

            if (self.peek_tok.type == .SYMBOL_COMMA) {
                self.nextToken();
            } else if (self.peek_tok.type == .SYMBOL_RPAREN) {
                self.nextToken();
                break;
            } else {
                return ParseError.UnexpectedToken;
            }
        }

        try self.expectPeek(.SYMBOL_SEMICOLON);

        return .{
            .INSERT = .{
                .table_name = table_name,
                .values = try val_list.toOwnedSlice(self.allocator),
            },
        };
    }

    fn parseSelect(self: *Self) !Statement {
        try self.expectPeek(.SYMBOL_ASTERISK);
        try self.expectPeek(.KEYWORD_FROM);
        try self.expectPeek(.IDENTIFIER);

        const table_name = self.curr_tok.lexeme;
        var where_expr: ?*Expression = null;
        if (self.peek_tok.type == .KEYWORD_WHERE) {
            self.nextToken();
            self.nextToken();
            where_expr = try self.parseExpr();
        }
        try self.expectPeek(.SYMBOL_SEMICOLON);

        return Statement{
            .SELECT = .{
                .table_name = table_name,
                .where = where_expr,
            },
        };
    }

    fn parseExpr(self: *Self) !*Expression {
        if (self.curr_tok.type != .IDENTIFIER) return ParseError.UnexpectedToken;
        const left_expr = try self.allocator.create(Expression);
        left_expr.* = .{ .Column = self.curr_tok.lexeme };
        self.nextToken();

        const op = switch (self.curr_tok.type) {
            .SYMBOL_EQUALS => Operator.Equals,
            .SYMBOL_GT => Operator.GreaterThan,
            .SYMBOL_GTE => Operator.GTE,
            .SYMBOL_LT => Operator.LessThan,
            .SYMBOL_LTE => Operator.LTE,
            .SYMBOL_NEQ => Operator.NotEquals,
            else => return ParseError.UnexpectedToken,
        };

        self.nextToken();
        const right_expr = try self.allocator.create(Expression);
        if (self.curr_tok.type == .LITERAL_NUMBER) {
            const val = try std.fmt.parseInt(i64, self.curr_tok.lexeme, 10);
            right_expr.* = .{ .Literal = .{ .integer = val } };
        } else if (self.curr_tok.type == .LITERAL_STRING) {
            right_expr.* = .{ .Literal = .{ .text = self.curr_tok.lexeme } };
        } else {
            return ParseError.UnexpectedToken;
        }

        const bin_expr = try self.allocator.create(Expression);
        bin_expr.* = .{
            .Binary = .{
                .left = left_expr,
                .op = op,
                .right = right_expr,
            },
        };

        return bin_expr;
    }
};

test "parser: basic insert statement" {
    const sql = "INSERT INTO users VALUES (1, 'kraytos');";
    var p = Parser.init(testing.allocator, sql);

    const stmt = try p.parse();
    defer {
        switch (stmt) {
            .INSERT => |i| testing.allocator.free(i.values),
            else => {},
        }
    }

    switch (stmt) {
        .INSERT => |ins| {
            try testing.expectEqualStrings("users", ins.table_name);
            try testing.expectEqual(2, ins.values.len);
            try testing.expectEqual(1, ins.values[0].integer);
            try testing.expectEqualStrings("kraytos", ins.values[1].text);
        },
        .SELECT => try testing.expect(false),
    }
}

test "parser: basic select statement" {
    const sql = "SELECT * FROM products;";
    var p = Parser.init(testing.allocator, sql);

    const stmt = try p.parse();
    defer {
        switch (stmt) {
            .INSERT => |i| testing.allocator.free(i.values),
            else => {},
        }
    }

    switch (stmt) {
        .SELECT => |sel| {
            try testing.expectEqualStrings("products", sel.table_name);
        },
        .INSERT => try testing.expect(false),
    }
}

test "parser: error on missing semicolon" {
    const sql = "SELECT * FROM users";
    var p = Parser.init(testing.allocator, sql);
    const result = p.parse();
    try testing.expectError(ParseError.UnexpectedToken, result);
}

test "parser: error on malformed insert" {
    const sql = "INSERT INTO users VALUES (1, );";
    var p = Parser.init(testing.allocator, sql);
    const result = p.parse();
    try testing.expectError(ParseError.UnexpectedToken, result);
}

test "parser: select with where clause" {
    const sql = "SELECT * FROM users WHERE id = 5;";
    var p = Parser.init(testing.allocator, sql);
    const stmt = try p.parse();
    defer {
        switch (stmt) {
            .SELECT => |sel| {
                if (sel.where) |expr| {
                    testing.allocator.destroy(expr.Binary.left);
                    testing.allocator.destroy(expr.Binary.right);
                    testing.allocator.destroy(expr);
                }
            },
            else => {},
        }
    }

    switch (stmt) {
        .SELECT => |sel| {
            try testing.expectEqualStrings("users", sel.table_name);
            try testing.expect(sel.where != null);

            const expr = sel.where.?;
            switch (expr.*) {
                .Binary => |b| {
                    try testing.expectEqual(Operator.Equals, b.op);
                    try testing.expectEqualStrings("id", b.left.Column);
                    try testing.expectEqual(5, b.right.Literal.integer);
                },
                else => try testing.expect(false),
            }
        },
        else => try testing.expect(false),
    }
}
