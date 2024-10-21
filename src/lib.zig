const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const c = @cImport({
    @cInclude("mysql.h");
});
const print = std.debug.print;
const debug = false;

pub const DBInfo = struct {
    host: [:0]const u8,
    user: [:0]const u8,
    password: [:0]const u8,
    database: [:0]const u8,
    port: u32 = 3306,
};

pub const ResultSet = struct {
    const Self = @This();
    allocator: Allocator,
    rows: ArrayList(*Row),

    pub fn init(allocator: Allocator) !*Self {
        const r = try allocator.create(ResultSet);
        r.* = .{
            .allocator = allocator,
            .rows = ArrayList(*Row).init(allocator),
        };
        return r;
    }

    pub fn addRow(self: *Self, column_count: u32) !*Row {
        const row = try Row.init(self.allocator, column_count);
        try self.rows.append(row);
        return row;
    }
    pub fn deinit(self: *Self) void {
        for (self.rows.items) |row| {
            row.deinit();
        }
        self.rows.deinit();
        self.allocator.destroy(self);
    }
};

const Row = struct {
    const Self = @This();
    allocator: Allocator,
    columns: ArrayList(?[]u8),

    pub fn init(allocator: Allocator, size: u32) !*Self {
        const r = try allocator.create(Row);
        r.* = .{
            .allocator = allocator,
            .columns = try ArrayList(?[]u8).initCapacity(allocator, size),
        };
        return r;
    }
    pub fn deinit(self: *Self) void {
        for (self.columns.items) |column| {
            if (column) |val| {
                self.allocator.free(val);
            }
        }
        self.columns.deinit();
        self.allocator.destroy(self);
    }
};

pub const DB = struct {
    conn: *c.MYSQL,
    allocator: Allocator,

    pub fn init(allocator: Allocator, db_info: DBInfo) !DB {
        const db = c.mysql_init(null);

        if (db == null) {
            return error.initError;
        }

        if (c.mysql_real_connect(
            db,
            db_info.host,
            db_info.user,
            db_info.password,
            db_info.database,
            db_info.port,
            null,
            c.CLIENT_MULTI_STATEMENTS,
        ) == null) {
            std.log.err("Connect to database failed: {s}\n", .{c.mysql_error(db)});
            return error.connectError;
        }

        return .{
            .conn = db,
            .allocator = allocator,
        };
    }

    fn deinit(self: DB) void {
        c.mysql_close(self.conn);
    }

    fn execute(self: DB, query: []const u8) !void {
        if (c.mysql_real_query(self.conn, query.ptr, query.len) != 0) {
            print("Exec query failed: {s}\n", .{c.mysql_error(self.conn)});
            return error.execError;
        }
    }

    pub fn executeAll(self: DB, query: []const u8) !void {
        if (c.mysql_real_query(self.conn, query.ptr, query.len) != 0) {
            print("Exec query failed: {s}\n", .{c.mysql_error(self.conn)});
            return error.execError;
        }

        while (c.mysql_next_result(self.conn) == 0) {
            const res = c.mysql_store_result(self.conn);
            c.mysql_free_result(res);
        }
    }

    pub fn columnCount(meta: *c.MYSQL_RES) u32 {
        const column_count: u32 = @intCast(c.mysql_num_fields(meta));
        return column_count;
    }

    pub fn runPreparedStatement(self: DB, allocator: Allocator, query: []const u8, params: anytype) !*ResultSet {
        const rs = try ResultSet.init(allocator);
        const stmt: *c.MYSQL_STMT = blk: {
            const stmt = c.mysql_stmt_init(self.conn);
            if (stmt == null) {
                return error.initStmt;
            }
            errdefer _ = c.mysql_stmt_close(stmt);

            if (c.mysql_stmt_prepare(stmt, @ptrCast(query), query.len) != 0) {
                std.log.err("Prepare color stmt failed, msg:{s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }

            break :blk stmt.?;
        };

        defer _ = c.mysql_stmt_close(stmt);

        const hasParams = params.len > 0;
        if (hasParams) {
            var param_binds = try allocator.alloc(c.MYSQL_BIND, params.len);
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
                std.log.err("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }
        }

        if (c.mysql_stmt_execute(stmt) != 0) {
            //std.log.err("Exec stmt failed: {s}\n", .{c.mysql_error(self.conn)});
            return error.execStmtError;
        }

        const metadata = c.mysql_stmt_result_metadata(stmt);
        if (metadata == null) {
            return rs;
        }

        const columns = c.mysql_fetch_fields(metadata);

        const cols = columnCount(metadata);

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
            print("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
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
                std.debug.print("Statement error: {d} \n", .{status});
                // showStatementError(statement);
            } else if (status == c.MYSQL_DATA_TRUNCATED) {
                std.debug.print("WARNING!!!  Statement data truncated: {d} \n", .{status});
            }

            if (!proceed) break;

            std.log.debug("error: {d}, length: {d}, is_null: {d} \n", .{ err, length, is_null });
            rowCount = rowCount + 1;
            const row = try rs.addRow(cols);
            for (0..cols) |i| {
                if (is_null[i] == 1) {
                    std.log.debug("Row data is NULL \n", .{});
                    try row.columns.append(null);
                } else {
                    const output_data = try allocator.alloc(u8, length[i]);
                    try row.columns.append(output_data);

                    //  try rw.columns.?.initAndSetBuffer(row[i][0..lengths[i]], i);
                    switch (r_binds[i].buffer_type) {
                        c.MYSQL_TYPE_TINY => {
                            const data: *u8 = @as(*u8, @ptrCast(@constCast(r_binds[i].buffer)));
                            std.log.debug("Row data Tiny: {d} \n", .{data.*});
                            output_data[0] = data.*;
                        },
                        else => {
                            const data: [*c]const u8 = @as([*c]u8, @ptrCast(@constCast(@alignCast(r_binds[i].buffer))));
                            std.log.debug("Row data String: {s} \n", .{data});
                            @memcpy(output_data[0..length[i]], data[0..length[i]]);
                        },
                    }
                }
            }
        }
        printResultSet(rs);
        return rs;
    }
};

var testdb: DB = undefined;
var testarena: std.heap.ArenaAllocator = undefined;

test "connect" {
    testarena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const testallocator = testarena.allocator();

    testdb = try DB.init(testallocator, .{
        .database = "",
        .host = "172.17.0.1",
        .user = "root",
        .password = "my-secret-pw",
    });
}

test "read data select from table" {
    //    const testallocator = testarena.allocator();
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
    const testallocator = testarena.allocator();
    const query =
        \\ SELECT NULL;
    ;
    const params = .{};
    const rs = try testdb.runPreparedStatement(testallocator, query, params);
    printResultSet(rs);
    try std.testing.expectEqual(null, rs.rows.items[0].columns.items[0]);
}

// test "simple select prepared statement" {
//     const testallocator = testarena.allocator();
//     const query = "SELECT 'just a happy test';";
//     const params = .{};
//     try testdb.runPreparedStatement(testallocator, query, params);
// }

test "simple select prepared statement 2 columns" {
    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test', 'more info';";
    const params = .{};
    _ = try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement with single param" {
    const testallocator = testarena.allocator();
    const query = "SELECT ?  as test";
    const params = .{"going on"};
    _ = try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement with single param 2" {
    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test' , ? as inparam;";
    const params = .{"going on"};
    _ = try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement with single param 3" {
    const testallocator = testarena.allocator();
    const params = .{"going on"};
    const query =
        \\ SELECT 'just a happy test' ,
        \\ ? as inparam;
    ;
    const rs = try testdb.runPreparedStatement(testallocator, query, params);

    try std.testing.expectEqualStrings("just a happy test", rs.rows.items[0].columns.items[0].?);
    try std.testing.expectEqualStrings("going on", rs.rows.items[0].columns.items[1].?);
}

test "expect fail simple select prepared statement with single param" {
    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test' , ? as inparam;";
    const params = .{};
    const res = testdb.runPreparedStatement(testallocator, query, params);
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

test "insert prepared statement" {
    const testallocator = testarena.allocator();
    const params = .{ "mike", true };
    const query = "INSERT INTO testtbl (name,active,timestamp) VALUES (?,?,NOW())";
    _ = try testdb.runPreparedStatement(testallocator, query, params);
}

test "insert multiple prepared statements" {
    const testallocator = testarena.allocator();
    const names = .{ "Mike", "John", "Lucky" };
    const query = "INSERT INTO testtbl (name,active, timestamp ) VALUES (?, false, NOW())";
    inline for (names) |name| {
        _ = try testdb.runPreparedStatement(testallocator, query, .{name});
    }
}

test "select from table" {
    const testallocator = testarena.allocator();
    const query =
        \\ SELECT *
        \\ FROM testtbl
        \\ WHERE name = ?;
    ;
    const params = .{"Mike"};
    const rs = try testdb.runPreparedStatement(testallocator, query, params);
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
