const std = @import("std");
const testing = std.testing;
const Token = @import("token.zig").Token;
const TokenType = @import("token.zig").TokenType;

const keywords = std.StaticStringMap(TokenType).initComptime(.{
    .{ "select", .KEYWORD_SELECT },
    .{ "insert", .KEYWORD_INSERT },
    .{ "into", .KEYWORD_INTO },
    .{ "values", .KEYWORD_VALUES },
    .{ "from", .KEYWORD_FROM },
    .{ "create", .KEYWORD_CREATE },
    .{ "table", .KEYWORD_TABLE },
    .{ "where", .KEYWORD_WHERE },
    .{ "and", .KEYWORD_AND },
    .{ "or", .KEYWORD_OR },
    .{ "SELECT", .KEYWORD_SELECT },
    .{ "INSERT", .KEYWORD_INSERT },
    .{ "INTO", .KEYWORD_INTO },
    .{ "VALUES", .KEYWORD_VALUES },
    .{ "FROM", .KEYWORD_FROM },
    .{ "CREATE", .KEYWORD_CREATE },
    .{ "TABLE", .KEYWORD_TABLE },
    .{ "WHERE", .KEYWORD_WHERE },
    .{ "AND", .KEYWORD_AND },
    .{ "OR", .KEYWORD_OR },
});

pub const Tokenizer = struct {
    source: []const u8,
    pos: usize = 0,
    read_pos: usize = 0,
    ch: u8 = 0,
    line: usize = 1,
    col: usize = 0,

    const Self = @This();

    pub fn init(source: []const u8) Self {
        var t = Self{ .source = source };
        t.readChar();
        return t;
    }

    pub fn next(self: *Self) Token {
        self.skipWhitespace();

        const start_pos = self.pos;
        const start_col = self.col;
        var token_type: TokenType = .ILLEGAL;
        switch (self.ch) {
            '(' => token_type = .SYMBOL_LPAREN,
            ')' => token_type = .SYMBOL_RPAREN,
            ',' => token_type = .SYMBOL_COMMA,
            ';' => token_type = .SYMBOL_SEMICOLON,
            '*' => token_type = .SYMBOL_ASTERISK,
            0 => token_type = .EOF,
            '=' => token_type = .SYMBOL_EQUALS,
            '>' => {
                if (self.peekChar() == '=') {
                    self.readChar();
                    token_type = .SYMBOL_GTE;
                } else {
                    token_type = .SYMBOL_GT;
                }
            },
            '<' => {
                if (self.peekChar() == '=') {
                    self.readChar();
                    token_type = .SYMBOL_LTE;
                } else if (self.peekChar() == '>') {
                    self.readChar();
                    token_type = .SYMBOL_NEQ;
                } else {
                    token_type = .SYMBOL_LT;
                }
            },
            '!' => {
                if (self.peekChar() == '=') {
                    self.readChar();
                    token_type = .SYMBOL_NEQ;
                } else {
                    token_type = .ILLEGAL;
                }
            },
            '\'' => return self.readString(),
            else => {
                if (std.ascii.isAlphabetic(self.ch) or self.ch == '_') {
                    return self.readIdentifier();
                } else if (std.ascii.isDigit(self.ch)) {
                    return self.readNumber();
                }
            },
        }

        self.readChar();
        const lexeme = if (token_type == .EOF) "" else self.source[start_pos..self.pos];

        return Token{
            .type = token_type,
            .lexeme = lexeme,
            .line = self.line,
            .column = start_col,
        };
    }

    fn readChar(self: *Self) void {
        if (self.read_pos >= self.source.len) {
            self.ch = 0;
        } else {
            self.ch = self.source[self.read_pos];
        }

        self.pos = self.read_pos;
        self.read_pos += 1;
        self.col += 1;
    }

    fn peekChar(self: *Self) u8 {
        if (self.read_pos >= self.source.len) return 0;
        return self.source[self.read_pos];
    }

    fn skipWhitespace(self: *Self) void {
        while (self.ch == ' ' or self.ch == '\t' or self.ch == '\n' or self.ch == '\r') {
            if (self.ch == '\n') {
                self.line += 1;
                self.col = 0;
            }
            self.readChar();
        }
    }

    fn readIdentifier(self: *Self) Token {
        const position = self.pos;
        const start_col = self.col;

        while (std.ascii.isAlphabetic(self.ch) or std.ascii.isDigit(self.ch) or self.ch == '_') {
            self.readChar();
        }

        const literal = self.source[position..self.pos];
        const t_type = keywords.get(literal) orelse .IDENTIFIER;
        return Token{
            .type = t_type,
            .lexeme = literal,
            .line = self.line,
            .column = start_col,
        };
    }

    fn readNumber(self: *Self) Token {
        const position = self.pos;
        const start_col = self.col;
        while (std.ascii.isDigit(self.ch)) {
            self.readChar();
        }

        if (self.ch == '.' and std.ascii.isDigit(self.peekChar())) {
            self.readChar();
            while (std.ascii.isDigit(self.ch)) {
                self.readChar();
            }
        }

        return Token{
            .type = .LITERAL_NUMBER,
            .lexeme = self.source[position..self.pos],
            .line = self.line,
            .column = start_col,
        };
    }

    fn readString(self: *Self) Token {
        const position = self.pos + 1;
        const start_col = self.col;
        while (true) {
            self.readChar();
            if (self.ch == '\'') {
                if (self.peekChar() == '\'') {
                    self.readChar();
                } else {
                    break;
                }
            } else if (self.ch == 0) {
                break;
            }
        }

        const literal = self.source[position..self.pos];
        if (self.ch == '\'') self.readChar();
        return Token{
            .type = .LITERAL_STRING,
            .lexeme = literal,
            .line = self.line,
            .column = start_col,
        };
    }
};

test "tokenizer basic select" {
    const input = "select * from users;";
    var t = Tokenizer.init(input);

    const t1 = t.next();
    try testing.expectEqual(TokenType.KEYWORD_SELECT, t1.type);

    const t2 = t.next();
    try testing.expectEqual(TokenType.SYMBOL_ASTERISK, t2.type);

    const t3 = t.next();
    try testing.expectEqual(TokenType.KEYWORD_FROM, t3.type);

    const t4 = t.next();
    try testing.expectEqual(TokenType.IDENTIFIER, t4.type);
    try testing.expectEqualStrings("users", t4.lexeme);

    const t5 = t.next();
    try testing.expectEqual(TokenType.SYMBOL_SEMICOLON, t5.type);

    const t6 = t.next();
    try testing.expectEqual(TokenType.EOF, t6.type);
}

test "tokenizer complex query" {
    const input = "SELECT * FROM users WHERE age >= 18 AND name = 'O''Reilly';";
    var t = Tokenizer.init(input);
    const expected_types = [_]TokenType{
        .KEYWORD_SELECT,
        .SYMBOL_ASTERISK,
        .KEYWORD_FROM,
        .IDENTIFIER,
        .KEYWORD_WHERE,
        .IDENTIFIER,
        .SYMBOL_GTE,
        .LITERAL_NUMBER,
        .KEYWORD_AND,
        .IDENTIFIER,
        .SYMBOL_EQUALS,
        .LITERAL_STRING,
        .SYMBOL_SEMICOLON,
        .EOF,
    };

    for (expected_types) |expected| {
        const tok = t.next();
        try testing.expectEqual(expected, tok.type);
    }
}
