const std = @import("std");
const Allocator = std.mem.Allocator;

const Conn = @import("lib.zig").Conn;
const testing = std.testing;

const ArrayList = std.ArrayList;

pub const Pool = struct {
    const Self = @This();
    allocator: Allocator,
    connections: ArrayList(*Conn),
    size: u32,
    _mutex: std.Thread.Mutex,
    _condition: std.Thread.Condition,
    _timeout: usize,

    pub fn init(allocator: Allocator, size: u32) !*Self {
        const r = try allocator.create(Self);
        errdefer allocator.destroy(r);
        const connections = ArrayList(*Conn).init(allocator);
        errdefer allocator.destroy(connections);
        r.* = .{
            .allocator = allocator,
            .connections = connections,
            .size = size,
            ._mutex = .{},
            ._condition = .{},
            ._timeout = 1 * std.time.ns_per_s,
        };
        return r;
    }

    pub fn deinit(self: *Self) void {
        for (self.connections.items) |conn| {
            conn.deinit();
        }
        self.connections.deinit();
        self.allocator.destroy(self);
    }

    fn getFreeConn(self: *Self) ?*Conn {
        for (self.connections.items) |conn| {
            if (!conn.busy) {
                conn.busy = true;
                return conn;
            }
        }
        return null;
    }
    fn printPoolStats(self: *Self) void {
        var free: u8 = 0;

        for (self.connections.items) |conn| {
            if (!conn.busy) {
                free += 1;
            }
        }

        std.debug.print("Size: {d}, free: {d}\n", .{ self.connections.items.len, free });
    }

    pub fn get(self: *Self, id: usize) !*Conn {
        _ = id;
        if (self._get()) |conn| {
            return conn;
        } else |err| {
            return err;
        }
    }
    fn _get(self: *Self) !*Conn {
        const id = std.Thread.getCurrentId();
        self.printPoolStats();
        self._mutex.lock();
        defer self._mutex.unlock();
        if (self.connections.items.len < self.size) {
            std.debug.print("get({d}) create new\n", .{id});
            var conn = try Conn.init(self.allocator, .{
                .database = "",
                .host = "172.17.0.1",
                .user = "root",
                .password = "my-secret-pw",
            }, self);
            conn.busy = true;
            try self.connections.append(conn);
            return conn;
        }
        const max_fails = 3;
        var retry_counter: u8 = 0;
        while (retry_counter < max_fails) {
            if (self.getFreeConn()) |val| {
                std.debug.print("get({}) found free \n", .{id});
                return val;
            } else {
                std.debug.print("get({}) wait for condition \n", .{id});
                var timer = try std.time.Timer.start();
                self._condition.timedWait(&self._mutex, self._timeout) catch |err| {
                    std.debug.print("get({}) timeout  {}\n", .{ id, err });
                    return error.poolBusy;
                };
                const elapsed = timer.lap();
                std.debug.print("get({}) wait over {d} \n", .{ id, elapsed });
                retry_counter += 1;
            }
        }

        return error.poolBusy;
    }

    pub fn release(self: *Self, conn: *Conn) void {
        if (true) {
            const id = std.Thread.getCurrentId();
            self._mutex.lock();
            std.debug.print("release({}) \n", .{id});
            // if (conn.dirty) {
            //     conn.deinit();
            //     self.connections.
            // }
            conn.busy = false;
            self._condition.signal();
            self._mutex.unlock();
        } else {
            conn.busy = false;
        }
    }
};

test "get connection" {
    const allocator = std.testing.allocator;
    const pool = try Pool.init(allocator, 5);
    const conn: *Conn = try pool.get(288);
    const rs = try conn.runPreparedStatement(allocator, "Select \"hello\" as greeting ", .{});
    defer rs.deinit();
    defer pool.deinit();
    std.debug.print("Res : {s} \n", .{rs.rows.items[0].columns.items[0].?});
    try std.testing.expectEqualStrings("hello", rs.rows.items[0].columns.items[0].?);
}

var testarena: std.heap.ArenaAllocator = undefined;

test "twice" {
    const allocator = std.testing.allocator;

    const pool = try Pool.init(allocator, 5);
    defer pool.deinit();
    const conn: *Conn = try pool.get(289);

    {
        const rs = try conn.runPreparedStatement(allocator, "Select \"hello\" as greeting ", .{});
        defer rs.deinit();
        std.debug.print("Res : {s} \n", .{rs.rows.items[0].columns.items[0].?});
        try std.testing.expectEqualStrings("hello", rs.rows.items[0].columns.items[0].?);
    }
    {
        const rs = try conn.runPreparedStatement(allocator, "Select \"hello\" as greeting ", .{});
        defer rs.deinit();
        std.debug.print("Res : {s} \n", .{rs.rows.items[0].columns.items[0].?});
        try std.testing.expectEqualStrings("hello", rs.rows.items[0].columns.items[0].?);
    }
}

var err_counter: usize = 0;
var prepared_fail: usize = 0;
var get_fail: usize = 0;
var ok_counter: usize = 0;
fn testConnFromPool(pool: *Pool, run: *bool) !void {
    while (run.*) {
        const id = std.Thread.getCurrentId();
        const conn = pool.get(id) catch |err| {
            std.debug.print("get({}) error: {?}\n", .{ id, err });
            get_fail += 1;
            continue;
        };
        //defer conn.release();
        // if (conn == null) {
        //     get_fail += 1;
        //     continue;
        // }
        const rs = conn.runPreparedStatement(std.testing.allocator, "Select \"hello\" as greeting ", .{}) catch {
            prepared_fail += 1;
            conn.release();
            continue;
        };
        defer rs.deinit();
        if (rs.rows.items.len < 1) {
            prepared_fail += 1;
            conn.release();
            continue;
        }
        if (std.mem.eql(u8, "hello", rs.rows.items[0].columns.items[0].?)) {
            ok_counter += 1;
        } else {
            err_counter += 1;
        }
        std.Thread.sleep(std.time.ns_per_s / 5);
        conn.release();
        //  std.Thread.sleep(1);
    }
}

test "start 10 threads to test the pool" {
    const runners = 10;
    const allocator = std.testing.allocator;
    const pool = try Pool.init(allocator, 5);
    defer pool.deinit();
    var run = true;
    for (0..runners) |i| {
        std.debug.print("Start thread  : {d}\n", .{i});
        _ = try std.Thread.spawn(.{}, testConnFromPool, .{ pool, &run });
    }
    std.time.sleep(5 * std.time.ns_per_s);
    run = false;
    std.time.sleep(1 * std.time.ns_per_s);
    std.debug.print("OK: {d}, ERROR: {d},  GET_FAIL: {d}, PREPARED_FAIL {d} \n", .{ ok_counter, err_counter, get_fail, prepared_fail });
}
