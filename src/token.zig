const std = @import("std");

pub const TokenType = enum {
    KEYWORD_SELECT,
    KEYWORD_INSERT,
    KEYWORD_INTO,
    KEYWORD_VALUES,
    KEYWORD_CREATE,
    KEYWORD_TABLE,
    KEYWORD_FROM,
    KEYWORD_WHERE,
    KEYWORD_AND,
    KEYWORD_OR,
    SYMBOL_LPAREN,
    SYMBOL_RPAREN,
    SYMBOL_COMMA,
    SYMBOL_SEMICOLON,
    SYMBOL_ASTERISK,
    SYMBOL_EQUALS,
    SYMBOL_GT,
    SYMBOL_GTE,
    SYMBOL_LT,
    SYMBOL_LTE,
    SYMBOL_NEQ,
    IDENTIFIER,
    LITERAL_STRING,
    LITERAL_NUMBER,
    ILLEGAL,
    EOF,

    pub fn isComparison(self: TokenType) bool {
        return switch (self) {
            .SYMBOL_EQUALS, .SYMBOL_GT, .SYMBOL_GTE, .SYMBOL_LT, .SYMBOL_LTE, .SYMBOL_NEQ => true,
            else => false,
        };
    }
};

pub const Token = struct {
    type: TokenType,
    lexeme: []const u8,
    line: usize,
    column: usize,

    pub fn format(
        self: Token,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: std.Io.Writer,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print("[{s}] \"{s}\" at {d}:{d}", .{ @tagName(self.type), self.lexeme, self.line, self.column });
    }
};
