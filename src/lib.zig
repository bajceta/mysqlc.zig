const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @cImport({
    @cInclude("mysql.h");
});
const print = std.debug.print;

pub const DBInfo = struct {
    host: [:0]const u8,
    user: [:0]const u8,
    password: [:0]const u8,
    database: [:0]const u8,
    port: u32 = 3306,
};

pub const DB = struct {
    conn: *c.MYSQL,
    allocator: Allocator,

    fn init(allocator: Allocator, db_info: DBInfo) !DB {
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
            print("Connect to database failed: {s}\n", .{c.mysql_error(db)});
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

    fn queryTable(self: DB) !void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        const query =
            \\ SELECT
            \\     "hello world for a long time, now" as Test,
            \\   "sec value" as val2,
            \\   "sec value" as val3,
            \\     "hello world for a long time, now" as Test2;
            \\
        ;
        const stmt: *c.MYSQL_STMT = blk: {
            const stmt = c.mysql_stmt_init(self.conn);
            if (stmt == null) {
                return error.initStmt;
            }
            errdefer _ = c.mysql_stmt_close(stmt);

            if (c.mysql_stmt_prepare(stmt, query, query.len) != 0) {
                print("Prepare stmt failed, msg:{s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }

            break :blk stmt.?;
        };
        const params = false;
        if (params) {
            const name = "Oreo";
            var param_binds = [_]c.MYSQL_BIND{std.mem.zeroes(c.MYSQL_BIND)};
            param_binds[0].buffer_type = c.MYSQL_TYPE_STRING;
            param_binds[0].buffer_length = name.len;
            param_binds[0].is_null = 0;
            param_binds[0].buffer = @constCast(@ptrCast(&name));
            if (c.mysql_stmt_bind_param(stmt, &param_binds) != 0) {
                print("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }
        }

        if (c.mysql_stmt_execute(stmt) != 0) {
            print("Exec color stmt failed: {s}\n", .{c.mysql_error(self.conn)});
            return error.execStmtError;
        }

        const metadata = c.mysql_stmt_result_metadata(stmt);
        const columns = c.mysql_fetch_fields(metadata);

        defer _ = c.mysql_stmt_close(stmt);
        const cols = 4;

        const buffers: [][]u8 = try allocator.alloc([]u8, cols);
        var length: []c_ulong = try allocator.alloc(c_ulong, cols);
        var is_null: []u8 = try allocator.alloc(u8, cols);
        var err: []u8 = try allocator.alloc(u8, cols);
        std.debug.print("PTRs  {d} , {d} {d} {d} \n", .{ &buffers, &length, &is_null, &err });

        var r_binds = try allocator.alloc(c.MYSQL_BIND, cols);
        for (0..cols) |i| {
            r_binds[i] = std.mem.zeroes(c.MYSQL_BIND);
            buffers[i] = try allocator.alloc(u8, columns[i].length);
            r_binds[i].buffer_length = columns[i].length;
            r_binds[i].buffer = @constCast(@ptrCast(@alignCast(buffers[i])));
            r_binds[i].is_null = @ptrCast(@alignCast(&is_null[i]));
            r_binds[i].length = @constCast(@ptrCast(&length[i]));
            r_binds[i].@"error" = @ptrCast(&err[i]);
            r_binds[i].buffer_type = c.MYSQL_TYPE_STRING;
            std.debug.print("binds: {any} \n", .{r_binds[i]});
        }

        //      std.debug.print("buffers: {any}\n", .{buffers});
        std.debug.print("error: {d}, length: {d}, is_null: {d} \n", .{ err, length, is_null });

        if (c.mysql_stmt_bind_result(stmt, @as([*c]c.MYSQL_BIND, @ptrCast(@alignCast(r_binds)))) != 0) {
            print("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
            return error.prepareStmt;
        }
        var rowCount: usize = 0;
        while (true) {
            const status = c.mysql_stmt_fetch(stmt);
            std.debug.print("status:  {d} ", .{status});
            const proceed = switch (status) {
                0, c.MYSQL_DATA_TRUNCATED => true,
                1, c.MYSQL_NO_DATA => false,
                else => false,
            };
            if (status == 1) {
                std.debug.print("Statement error: {d} \n", .{status});
                // showStatementError(statement);
            } else if (status == c.MYSQL_DATA_TRUNCATED) {
                std.debug.print("Statement data truncated: {d} \n", .{status});
            } else {
                std.debug.print("OK \n", .{});
            }

            if (!proceed) break;

            std.debug.print("error: {d}, length: {d}, is_null: {d} \n", .{ err, length, is_null });
            rowCount = rowCount + 1;
            for (0..cols) |i| {
                if (is_null[i] == 1) {
                    std.debug.print("Row data is NULL \n", .{});
                } else {
                    const c_string: [*c]const u8 = @as([*c]u8, @ptrCast(@constCast(@alignCast(r_binds[i].buffer))));
                    std.debug.print("Row data String: {s} \n", .{c_string});
                }
            }
        }
    }

    pub fn columnCount(meta: *c.MYSQL_RES) usize {
        const column_count = @as(usize, c.mysql_num_fields(meta));
        return column_count;
    }

    fn runPreparedStatement(self: DB, allocator: Allocator, query: []const u8, params: anytype) !void {
        const stmt: *c.MYSQL_STMT = blk: {
            const stmt = c.mysql_stmt_init(self.conn);
            if (stmt == null) {
                return error.initStmt;
            }
            errdefer _ = c.mysql_stmt_close(stmt);

            if (c.mysql_stmt_prepare(stmt, @ptrCast(query), query.len) != 0) {
                print("Prepare color stmt failed, msg:{s}\n", .{c.mysql_error(self.conn)});
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
                        print("Input param boolean: {b}  ", .{@intFromBool(param)});
                        param_binds[i].buffer = @ptrCast(@constCast(&@intFromBool(param)));
                        param_binds[i].buffer_length = 1;
                    },
                    else => {
                        param_binds[i].buffer_type = c.MYSQL_TYPE_STRING;
                        param_binds[i].buffer = @constCast(@ptrCast(param.ptr));
                        param_binds[i].buffer_length = param.len;
                        print("Param:{d} {s} , len: {d} \n", .{ i, param, param.len });
                    },
                }
                param_binds[i].is_null = 0;
                std.debug.print("Param binds: {any} \n", .{param_binds[i]});
            }
            if (c.mysql_stmt_bind_param(stmt, @ptrCast(@alignCast(param_binds))) != 0) {
                print("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }
        }

        if (c.mysql_stmt_execute(stmt) != 0) {
            print("Exec color stmt failed: {s}\n", .{c.mysql_error(self.conn)});
            return error.execStmtError;
        }

        const metadata = c.mysql_stmt_result_metadata(stmt);
        if (metadata == null) {
            return;
        }

        const columns = c.mysql_fetch_fields(metadata);

        const cols = columnCount(metadata);

        const buffers: [][]u8 = try allocator.alloc([]u8, cols);
        defer allocator.free(buffers);
        var length: []c_ulong = try allocator.alloc(c_ulong, cols);
        defer allocator.free(length);
        var is_null: []u8 = try allocator.alloc(u8, cols);
        defer allocator.free(is_null);
        var err: []u8 = try allocator.alloc(u8, cols);
        defer allocator.free(err);
        //std.debug.print("PTRs  {any} , {any} {d} {d} \n", .{ buffers.ptr, length.ptr, &is_null, &err });
        var r_binds = try allocator.alloc(c.MYSQL_BIND, cols);
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
            print("Column: {s}\n", .{columns[i].name});
            print("columns {any}\n", .{columns[i]});
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

            std.debug.print("error: {d}, length: {d}, is_null: {d} \n", .{ err, length, is_null });
            rowCount = rowCount + 1;
            for (0..cols) |i| {
                if (is_null[i] == 1) {
                    std.debug.print("Row data is NULL \n", .{});
                } else {
                    switch (r_binds[i].buffer_type) {
                        c.MYSQL_TYPE_TINY => {
                            const data: *u8 = @as(*u8, @ptrCast(@constCast(r_binds[i].buffer)));
                            std.debug.print("Row data Tiny: {d} \n", .{data.*});
                        },
                        else => {
                            const c_string: [*c]const u8 = @as([*c]u8, @ptrCast(@constCast(@alignCast(r_binds[i].buffer))));
                            std.debug.print("Row data String: {s} \n", .{c_string});
                        },
                    }
                }
            }
        }
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
test "simple select prepared statement" {
    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test';";
    const params = .{};
    try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement 2 columns" {
    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test', 'more info';";
    const params = .{};
    try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement with single param" {
    const testallocator = testarena.allocator();
    const query = "SELECT ?  as test";
    const params = .{"going on"};
    try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement with single param 2" {
    const testallocator = testarena.allocator();
    const query = "SELECT 'just a happy test' , ? as inparam;";
    const params = .{"going on"};
    try testdb.runPreparedStatement(testallocator, query, params);
}

test "simple select prepared statement with single param 3" {
    const testallocator = testarena.allocator();
    const params = .{"going on"};
    const query =
        \\ SELECT 'just a happy test' ,
        \\ ? as inparam;
    ;
    try testdb.runPreparedStatement(testallocator, query, params);
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
    try testdb.execute(
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
    while (c.mysql_next_result(testdb.conn) == 0) {
        const res = c.mysql_store_result(testdb.conn);
        c.mysql_free_result(res);
    }
}

test "insert prepared statement" {
    const testallocator = testarena.allocator();
    const params = .{ "mike", true };
    const query = "INSERT INTO testtbl (name,active,timestamp) VALUES (?,?,NOW())";
    try testdb.runPreparedStatement(testallocator, query, params);
}

test "insert multiple prepared statements" {
    const testallocator = testarena.allocator();
    const names = .{ "Mike", "John", "Lucky" };
    const query = "INSERT INTO testtbl (name,active, timestamp ) VALUES (?, false, NOW())";
    inline for (names) |name| {
        try testdb.runPreparedStatement(testallocator, query, .{name});
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
    try testdb.runPreparedStatement(testallocator, query, params);
}
