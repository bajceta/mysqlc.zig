const std = @import("std");
const Allocator = std.mem.Allocator;

const Conn = @import("lib.zig").Conn;
const log = @import("lib.zig").log;
const ConnectionOptions = @import("lib.zig").ConnectionOptions;

const testing = std.testing;

const ArrayList = std.ArrayList;

pub const PoolOptions = struct {
    size: u16,
    timeout: usize = 3 * std.time.ns_per_s,
    connection_options: ConnectionOptions,
};

const debug = false;
pub const Pool = struct {
    const Self = @This();
    allocator: Allocator,
    connections: ArrayList(*Conn),
    options: PoolOptions,
    _mutex: std.Thread.Mutex,
    _condition: std.Thread.Condition,
    _timeout: usize,
    _size: u16,

    pub fn init(allocator: Allocator, options: PoolOptions) !*Self {
        const r = try allocator.create(Self);
        errdefer allocator.destroy(r);
        const connections = ArrayList(*Conn).init(allocator);
        errdefer allocator.destroy(connections);
        r.* = .{
            .allocator = allocator,
            .connections = connections,
            .options = options,
            ._mutex = .{},
            ._condition = .{},
            ._timeout = options.timeout,
            ._size = options.size,
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

    fn getFreeConn(self: *Self) !*Conn {
        for (self.connections.items) |conn| {
            if (!conn.busy) {
                conn.busy = true;
                return conn;
            }
        }
        return error.poolBusy;
    }
    fn printPoolStats(self: *Self) void {
        var free: u8 = 0;

        for (self.connections.items) |conn| {
            if (!conn.busy) {
                free += 1;
            }
        }

        log.debug("Size: {d}, free: {d}\n", .{ self.connections.items.len, free });
    }

    pub fn get(self: *Self) !*Conn {
        return try self._get();
    }
    fn _get(self: *Self) !*Conn {
        const id = std.Thread.getCurrentId();
        if (debug) self.printPoolStats();
        self._mutex.lock();
        defer self._mutex.unlock();
        if (self.connections.items.len < self.options.size) {
            //std.debug.print("get({d}) create new\n", .{id});
            var conn = try Conn.init(self.allocator, self.options.connection_options, self);
            conn.busy = true;
            try self.connections.append(conn);
            return conn;
        }
        const max_fails = 3;
        var retry_counter: u8 = 0;
        var timer = try std.time.Timer.start();
        while (retry_counter < max_fails) {
            const conn = self.getFreeConn() catch {
                var elapsed = timer.read();
                //std.debug.print("get({}) wait for condition \n", .{id});
                self._condition.timedWait(&self._mutex, self._timeout - elapsed) catch {
                    //std.debug.print("get({}) timeout  {}\n", .{ id, err });
                    return error.poolTimeout;
                };
                elapsed = timer.read();
                if (debug) std.debug.print("get({}) wait over {d} \n", .{ id, elapsed });
                retry_counter += 1;
                continue;
            };
            return conn;
        }

        return self.getFreeConn();
    }

    pub fn release(self: *Self, conn: *Conn) void {
        if (true) {
            const id = std.Thread.getCurrentId();
            self._mutex.lock();
            if (debug) std.debug.print("release({}) \n", .{id});
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

const test_pool_options = PoolOptions{ .size = 5, .connection_options = ConnectionOptions{
    .database = "",
    .host = "172.17.0.1",
    .user = "root",
    .password = "my-secret-pw",
} };

test "get connection" {
    const allocator = std.testing.allocator;
    const pool = try Pool.init(allocator, test_pool_options);
    const conn: *Conn = try pool.get();
    const rs = try conn.runPreparedStatement(allocator, "Select \"hello\" as greeting ", .{});
    defer rs.deinit();
    defer pool.deinit();
    std.debug.print("Res : {s} \n", .{rs.rows.items[0].columns.items[0].?});
    try std.testing.expectEqualStrings("hello", rs.rows.items[0].columns.items[0].?);
}

var testarena: std.heap.ArenaAllocator = undefined;

test "twice" {
    const allocator = std.testing.allocator;

    const pool = try Pool.init(allocator, test_pool_options);
    defer pool.deinit();
    const conn: *Conn = try pool.get();

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

test "connect db" {
    testarena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const testallocator = testarena.allocator();

    const pool = try Pool.init(testallocator, PoolOptions{ .size = 2, .connection_options = .{
        .database = "testdb",
        .host = "172.17.0.1",
        .user = "root",
        .password = "my-secret-pw",
    } });
    defer pool.deinit();
    const query =
        \\ SELECT DATABASE();
    ;
    const params = .{};
    const conn = try pool.get();
    const rs = try conn.runPreparedStatement(std.testing.allocator, query, params);
    try std.testing.expectEqualStrings("testdb", rs.rows.items[0].columns.items[0].?);
    defer rs.deinit();
}

test "connect no db" {
    testarena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const testallocator = testarena.allocator();

    const pool = try Pool.init(testallocator, PoolOptions{ .size = 2, .connection_options = .{
        .database = "",
        .host = "172.17.0.1",
        .user = "root",
        .password = "my-secret-pw",
    } });
    defer pool.deinit();
    const query =
        \\ SELECT *
        \\ FROM testtbl
        \\ WHERE name = ?;
    ;
    const params = .{"Mike"};
    const conn = try pool.get();
    _ = conn.runPreparedStatement(std.testing.allocator, query, params) catch |err| {
        log.debug("Error: {?}", .{err});
        return;
    };
    try std.testing.expect(false);
}

var err_counter: usize = 0;
var prepared_fail: usize = 0;
var get_fail: usize = 0;
var ok_counter: usize = 0;
fn testConnFromPool(pool: *Pool, run: *bool) !void {
    while (run.*) {
        const id = std.Thread.getCurrentId();
        const conn = pool.get() catch |err| {
            log.warn("get({}) error: {?}\n", .{ id, err });
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
    const runners = 30;
    const allocator = std.testing.allocator;
    const pool = try Pool.init(allocator, test_pool_options);
    defer pool.deinit();
    var run = true;
    // var threads = [_]std.Thread{} ** runners;
    var threads: [runners]std.Thread = undefined;
    for (0..runners) |i| {
        threads[i] = try std.Thread.spawn(.{}, testConnFromPool, .{ pool, &run });
    }
    std.time.sleep(5 * std.time.ns_per_s);
    run = false;
    for (threads) |thread| {
        thread.join();
    }
    std.debug.print("OK: {d}, ERROR: {d},  GET_FAIL: {d}, PREPARED_FAIL {d} \n", .{ ok_counter, err_counter, get_fail, prepared_fail });
    try std.testing.expect(ok_counter > 125);
    try std.testing.expect(get_fail < 5);
    try std.testing.expect(err_counter == 0);
}
