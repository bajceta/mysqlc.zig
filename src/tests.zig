const conn = @import("conn.zig");
const pool = @import("pool.zig");
const std = @import("std");

test "global" {
    std.testing.log_level = std.log.Level.debug;
    _ = conn;
    _ = pool;
}
