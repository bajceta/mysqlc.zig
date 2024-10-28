const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const c = @cImport({
    @cInclude("mysql.h");
});
const print = std.debug.print;
const debug = false;

const Pool = @import("lib.zig").Pool;
const log = @import("lib.zig").log;
const Row = @import("lib.zig").Row;
const ResultSet = @import("lib.zig").ResultSet;

pub const ConnectionOptions = struct {
    database: []const u8,
    host: []const u8,
    user: []const u8,
    password: []const u8,
    port: u32 = 3306,
};

pub const Conn = struct {
    const Self = @This();

    _mysql: *c.MYSQL,
    allocator: Allocator,
    pool: ?*Pool,
    busy: bool,
    dirty: bool,

    pub fn init(allocator: Allocator, db_info: ConnectionOptions, pool: ?*Pool) !*Self {
        log.debug("Init mysql connection, host: {s}, user: {s}, database: {s}, port: {d} \n", .{ db_info.database, db_info.user, db_info.database, db_info.port });
        const conn = try allocator.create(Self);

        errdefer allocator.destroy(conn);

        conn.allocator = allocator;
        conn.pool = pool;
        conn.busy = false;

        try conn.connect(db_info);
        return conn;
    }

    pub fn disconnect(self: *Self) void {
        c.mysql_close(self._mysql);
    }

    pub fn connect(self: *Self, db_info: ConnectionOptions) !void {
        const _mysql = c.mysql_init(null);
        errdefer c.mysql_close(_mysql);

        if (_mysql == null) {
            return error.initError;
        }

        if (c.mysql_real_connect(
            _mysql,
            db_info.host.ptr,
            db_info.user.ptr,
            db_info.password.ptr,
            db_info.database.ptr,
            db_info.port,
            null,
            c.CLIENT_MULTI_STATEMENTS,
        ) == null) {
            log.warn("Connect to database failed: {s}\n", .{c.mysql_error(_mysql)});
            return error.connectError;
        }

        self._mysql = _mysql;
        self.dirty = false;
    }

    pub fn release(self: *Self) void {
        if (self.pool != null) {
            self.pool.?.release(self);
        }
    }

    pub fn deinit(self: *Self) void {
        c.mysql_close(self._mysql);
        self.allocator.destroy(self);
    }

    fn execute(self: *Self, query: []const u8) !void {
        if (c.mysql_real_query(self._mysql, query.ptr, query.len) != 0) {
            log.warn("Exec query failed: {s}\n", .{c.mysql_error(self._mysql)});
            return error.execError;
        }
    }

    pub fn executeAll(self: *Self, query: []const u8) !void {
        if (c.mysql_real_query(self._mysql, query.ptr, query.len) != 0) {
            log.warn("Exec query failed: {s}\n", .{c.mysql_error(self._mysql)});
            return error.execError;
        }

        while (true) {
            const status = c.mysql_next_result(self._mysql);
            switch (status) {
                0 => {
                    const res = c.mysql_store_result(self._mysql);
                    c.mysql_free_result(res);
                },
                -1 => {
                    break;
                },
                else => {
                    print("executeAll failed: {s}\n", .{c.mysql_error(self._mysql)});
                    return error.execError;
                },
            }
        }
    }

    pub fn runPreparedStatement(self: *Self, allocator: Allocator, query: []const u8, params: anytype) !*ResultSet {
        const rs = try ResultSet.init(allocator);
        errdefer rs.deinit();
        const stmt: *c.MYSQL_STMT = blk: {
            const stmt = c.mysql_stmt_init(self._mysql);
            if (stmt == null) {
                return error.initStmt;
            }
            errdefer _ = c.mysql_stmt_close(stmt);

            if (c.mysql_stmt_prepare(stmt, @ptrCast(query), query.len) != 0) {
                log.warn("Prepare color stmt failed, msg:{s}\n", .{c.mysql_error(self._mysql)});
                self.dirty = true;
                return error.prepareStmt;
            }

            break :blk stmt.?;
        };

        defer _ = c.mysql_stmt_close(stmt);

        const hasParams = params.len > 0;
        if (hasParams) {
            var param_binds = try allocator.alloc(c.MYSQL_BIND, params.len);
            defer allocator.free(param_binds);
            inline for (params, 0..) |param, i| {
                const T = @TypeOf(param);
                param_binds[i] = std.mem.zeroes(c.MYSQL_BIND);
                switch (T) {
                    bool => {
                        param_binds[i].buffer_type = c.MYSQL_TYPE_TINY;
                        if (debug) print("Input param boolean: {b}  ", .{@intFromBool(param)});
                        param_binds[i].buffer = @ptrCast(@constCast(&@intFromBool(param)));
                        param_binds[i].buffer_length = 1;
                    },
                    else => {
                        param_binds[i].buffer_type = c.MYSQL_TYPE_STRING;
                        param_binds[i].buffer = @constCast(@ptrCast(param.ptr));
                        param_binds[i].buffer_length = param.len;
                        if (debug) print("Param:{d} {s} , len: {d} \n", .{ i, param, param.len });
                    },
                }
                param_binds[i].is_null = 0;
                if (debug) std.debug.print("Param binds: {any} \n", .{param_binds[i]});
            }
            if (c.mysql_stmt_bind_param(stmt, @ptrCast(@alignCast(param_binds))) != 0) {
                log.warn("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self._mysql)});
                return error.prepareStmt;
            }
        }

        if (c.mysql_stmt_execute(stmt) != 0) {
            //std.log.err("Exec stmt failed: {s}\n", .{c.mysql_error(self._mysql)});
            return error.execStmtError;
        }

        const metadata = c.mysql_stmt_result_metadata(stmt);
        if (metadata == null) {
            return rs;
        }

        const columns = c.mysql_fetch_fields(metadata);

        const cols: u32 = @intCast(c.mysql_num_fields(metadata));
        // const cols = columnCount(metadata);

        const buffers: [][]u8 = try allocator.alloc([]u8, cols);
        defer allocator.free(buffers);
        defer {
            for (buffers) |buffer| {
                allocator.free(buffer);
            }
        }
        var length: []c_ulong = try allocator.alloc(c_ulong, cols);
        defer allocator.free(length);
        var is_null: []u8 = try allocator.alloc(u8, cols);
        defer allocator.free(is_null);
        var err: []u8 = try allocator.alloc(u8, cols);
        defer allocator.free(err);
        var r_binds = try allocator.alloc(c.MYSQL_BIND, cols);
        defer allocator.free(r_binds);
        for (0..cols) |i| {
            r_binds[i] = std.mem.zeroes(c.MYSQL_BIND);
            switch (columns[i].type) {
                c.MYSQL_TYPE_TINY => {
                    r_binds[i].buffer_type = c.MYSQL_TYPE_TINY;
                },
                else => {
                    r_binds[i].buffer_type = c.MYSQL_TYPE_STRING;
                },
            }
            if (debug) print("Column: {s}\n", .{columns[i].name});
            if (debug) print("columns {any}\n", .{columns[i]});
            buffers[i] = try allocator.alloc(u8, columns[i].length);
            r_binds[i].buffer_length = columns[i].length;
            r_binds[i].buffer = @constCast(@ptrCast(@alignCast(buffers[i])));
            r_binds[i].is_null = @ptrCast(@alignCast(&is_null[i]));
            r_binds[i].length = @constCast(@ptrCast(&length[i]));
            r_binds[i].@"error" = @ptrCast(&err[i]);
            //std.debug.print("binds: {any} \n", .{r_binds[i]});
        }

        if (c.mysql_stmt_bind_result(stmt, @as([*c]c.MYSQL_BIND, @ptrCast(@alignCast(r_binds)))) != 0) {
            log.warn("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self._mysql)});
            self.dirty = true;
            return error.prepareStmt;
        }
        var rowCount: usize = 0;
        while (true) {
            const status = c.mysql_stmt_fetch(stmt);
            //std.debug.print("status:  {d} ", .{status});
            const proceed = switch (status) {
                0, c.MYSQL_DATA_TRUNCATED => true,
                1, c.MYSQL_NO_DATA => false,
                else => false,
            };
            if (status == 1) {
                log.warn("Statement error: {d} \n", .{status});
                // showStatementError(statement);
            } else if (status == c.MYSQL_DATA_TRUNCATED) {
                log.warn("WARNING!!!  Statement data truncated: {d} \n", .{status});
            }

            if (!proceed) break;

            if (debug) log.debug("error: {d}, length: {d}, is_null: {d} \n", .{ err, length, is_null });
            rowCount = rowCount + 1;
            const row = try rs.addRow(cols);
            for (0..cols) |i| {
                if (is_null[i] == 1) {
                    if (debug) log.debug("Row data is NULL \n", .{});
                    try row.columns.append(null);
                } else {
                    const output_data = try row.allocator.alloc(u8, length[i]);
                    try row.columns.append(output_data);

                    //  try rw.columns.?.initAndSetBuffer(row[i][0..lengths[i]], i);
                    switch (r_binds[i].buffer_type) {
                        c.MYSQL_TYPE_TINY => {
                            const data: *u8 = @as(*u8, @ptrCast(@constCast(r_binds[i].buffer)));
                            if (debug) log.debug("Row data Tiny: {d} \n", .{data.*});
                            output_data[0] = data.*;
                        },
                        else => {
                            const data: [*c]const u8 = @as([*c]u8, @ptrCast(@constCast(@alignCast(r_binds[i].buffer))));
                            if (debug) log.debug("Row data String: {s} \n", .{data});
                            @memcpy(output_data[0..length[i]], data[0..length[i]]);
                        },
                    }
                }
            }
        }
        if (c.mysql_stmt_free_result(stmt) != 0) {
            log.warn("Failed to free statement results {s}", .{c.mysql_stmt_error(stmt)});
            self.dirty = true;
        }
        if (debug) printResultSet(rs);
        return rs;
    }
};

var testdb: *Conn = undefined;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
var testallocatorgpa = gpa.allocator();

test "connect 1" {
    testdb = try Conn.init(testallocatorgpa, .{
        .database = "",
        .host = "172.17.0.1",
        .user = "root",
        .password = "my-secret-pw",
    }, null);
}

test "read data select from table" {
    const allocator = std.testing.allocator;
    const query =
        \\ SELECT 'testres';
    ;
    const params = .{};
    const rs = try testdb.runPreparedStatement(allocator, query, params);
    defer rs.deinit();
    try std.testing.expectEqualStrings("testres", rs.rows.items[0].columns.items[0].?);
}

test "read data select from table 2" {
    const query =
        \\ SELECT NULL;
    ;
    const params = .{};
    const rs = try testdb.runPreparedStatement(std.testing.allocator, query, params);
    defer rs.deinit();
    printResultSet(rs);
    try std.testing.expectEqual(null, rs.rows.items[0].columns.items[0]);
}

test "simple select prepared statement 2 columns" {
    const query = "SELECT 'just a happy test', 'more info';";
    const params = .{};
    const rs = try testdb.runPreparedStatement(std.testing.allocator, query, params);
    rs.deinit();
}

test "simple select prepared statement with single param" {
    const query = "SELECT ?  as test";
    const params = .{"going on"};
    const rs = try testdb.runPreparedStatement(std.testing.allocator, query, params);
    rs.deinit();
}

test "simple select prepared statement with single param 2" {
    const query = "SELECT 'just a happy test' , ? as inparam;";
    const params = .{"going on"};
    const rs = try testdb.runPreparedStatement(std.testing.allocator, query, params);
    rs.deinit();
}

test "simple select prepared statement with single param 3" {
    const testallocator = std.testing.allocator;
    const params = .{"going on"};
    const query =
        \\ SELECT 'just a happy test' ,
        \\ ? as inparam;
    ;
    const rs = try testdb.runPreparedStatement(testallocator, query, params);
    try std.testing.expectEqualStrings("just a happy test", rs.rows.items[0].columns.items[0].?);
    try std.testing.expectEqualStrings("going on", rs.rows.items[0].columns.items[1].?);
    rs.deinit();
}
test "twice" {
    const testallocator = std.testing.allocator;
    const allocator = testallocator;

    {
        const rs = try testdb.runPreparedStatement(allocator, "Select \"hello\" as greeting ", .{});
        defer rs.deinit();
        std.debug.print("Res : {s} \n", .{rs.rows.items[0].columns.items[0].?});
        try std.testing.expectEqualStrings("hello", rs.rows.items[0].columns.items[0].?);
    }
    {
        const rs = try testdb.runPreparedStatement(allocator, "Select \"hello\" as greeting ", .{});
        defer rs.deinit();
        std.debug.print("Res : {s} \n", .{rs.rows.items[0].columns.items[0].?});
        try std.testing.expectEqualStrings("hello", rs.rows.items[0].columns.items[0].?);
    }
}

test "expect fail simple select prepared statement with single param" {
    //    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test' , ? as inparam;";
    const params = .{};
    const res = testdb.runPreparedStatement(std.testing.allocator, query, params);
    if (res) |_| {
        try std.testing.expect(false);
    } else |err| {
        try std.testing.expectEqual(error.execStmtError, err);
    }
}

test "create table prepared statement with single param 4" {
    try testdb.executeAll(
        \\ CREATE DATABASE IF NOT EXISTS testdb;
        \\ USE testdb;
        \\ DROP TABLE IF EXISTS testtbl;
        \\ CREATE TABLE IF NOT EXISTS testtbl (
        \\  id INT AUTO_INCREMENT PRIMARY KEY,
        \\  name VARCHAR(255) NOT NULL,
        \\  active BOOL NOT NULL,
        \\  timestamp TIMESTAMP NOT NULL,
        \\  maybe LONG
        \\ );
    );
}

test "expect error create table apns and gcm" {
    const res = testdb.executeAll(
        \\  DROP TABLE IF EXISTS test2
        \\  CREATE TABLE test2 (
        \\   `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
        \\   `name` varchar(200) DEFAULT NULL,
        \\  )
        \\  DROP TABLE IF EXISTS test2;
    );
    try std.testing.expectEqual(error.execError, res);
}

test "insert prepared statement" {
    const params = .{ "mike", true };
    const query = "INSERT INTO testtbl (name,active,timestamp) VALUES (?,?,NOW())";
    const rs = try testdb.runPreparedStatement(std.testing.allocator, query, params);
    rs.deinit();
}

test "insert multiple prepared statements" {
    const names = .{ "Mike", "John", "Lucky" };
    const query = "INSERT INTO testtbl (name,active, timestamp ) VALUES (?, false, NOW())";
    inline for (names) |name| {
        const rs = try testdb.runPreparedStatement(std.testing.allocator, query, .{name});
        rs.deinit();
    }
}

test "select from table" {
    const query =
        \\ SELECT *
        \\ FROM testtbl
        \\ WHERE name = ?;
    ;
    const params = .{"Mike"};
    const rs = try testdb.runPreparedStatement(std.testing.allocator, query, params);
    defer rs.deinit();
    printResultSet(rs);
}

test "connect testdb" {
    const conn = try Conn.init(std.testing.allocator, .{
        .database = "testdb",
        .host = "172.17.0.1",
        .user = "root",
        .password = "my-secret-pw",
    }, null);
    defer conn.deinit();
    const query =
        \\ SELECT DATABASE();
    ;
    const params = .{};
    const rs = try conn.runPreparedStatement(std.testing.allocator, query, params);
    defer rs.deinit();
    try std.testing.expectEqualStrings("testdb", rs.rows.items[0].columns.items[0].?);
    printResultSet(rs);
}

fn printResultSet(rs: *ResultSet) void {
    for (rs.rows.items) |row| {
        for (row.columns.items) |column| {
            if (column) |val| {
                std.debug.print("{s} ", .{val});
            } else {
                std.debug.print("{any} ", .{column});
            }
        }
        std.debug.print("\n", .{});
    }
}
