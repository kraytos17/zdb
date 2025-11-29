const std = @import("std");

// [ 48 bits unused ][ 16 bits slot ][ 32 bits page_id ]
pub const RecordRef = struct {
    page_id: u32,
    slot: u16,

    const Self = @This();

    pub fn encode(self: Self) u64 {
        return (@as(u64, self.page_id) << 16) | @as(u64, self.slot);
    }

    pub fn decode(v: u64) Self {
        return .{
            .page_id = @intCast(v >> 16),
            .slot = @intCast(v & 0xFFFF),
        };
    }
};
