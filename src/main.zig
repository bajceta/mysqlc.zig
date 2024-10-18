//! MySQL 8.0 API demo
//! https://dev.mysql.com/doc/c-api/8.0/en/c-api-basic-interface-usage.html
//!
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
                print("Prepare color stmt failed, msg:{s}\n", .{c.mysql_error(self.conn)});
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

    fn insertTable(self: DB) !void {
        const cat_colors = .{
            .{
                "Blue",
                .{ "Tigger", "Sammy" },
            },
            .{
                "Black",
                .{ "Oreo", "Biscuit" },
            },
        };

        const insert_color_stmt: *c.MYSQL_STMT = blk: {
            const stmt = c.mysql_stmt_init(self.conn);
            if (stmt == null) {
                return error.initStmt;
            }
            errdefer _ = c.mysql_stmt_close(stmt);

            const insert_color_query = "INSERT INTO cat_colors (name) values (?)";
            if (c.mysql_stmt_prepare(stmt, insert_color_query, insert_color_query.len) != 0) {
                print("Prepare color stmt failed, msg:{s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }

            break :blk stmt.?;
        };
        defer _ = c.mysql_stmt_close(insert_color_stmt);

        const insert_cat_stmt = blk: {
            const stmt = c.mysql_stmt_init(self.conn);
            if (stmt == null) {
                return error.initStmt;
            }
            errdefer _ = c.mysql_stmt_close(stmt);

            const insert_cat_query = "INSERT INTO cats (name, color_id) values (?, ?)";
            if (c.mysql_stmt_prepare(stmt, insert_cat_query, insert_cat_query.len) != 0) {
                print("Prepare cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
                return error.prepareStmt;
            }

            break :blk stmt.?;
        };
        defer _ = c.mysql_stmt_close(insert_cat_stmt);

        inline for (cat_colors) |row| {
            const color = row.@"0";
            const cat_names = row.@"1";

            var color_binds = [_]c.MYSQL_BIND{std.mem.zeroes(c.MYSQL_BIND)};
            color_binds[0].buffer_type = c.MYSQL_TYPE_STRING;
            color_binds[0].buffer_length = color.len;
            color_binds[0].is_null = 0;
            color_binds[0].buffer = @constCast(@ptrCast(color.ptr));

            if (c.mysql_stmt_bind_param(insert_color_stmt, &color_binds) != 0) {
                print("Bind color param failed: {s}\n", .{c.mysql_error(self.conn)});
                return error.bindParamError;
            }
            if (c.mysql_stmt_execute(insert_color_stmt) != 0) {
                print("Exec color stmt failed: {s}\n", .{c.mysql_error(self.conn)});
                return error.execStmtError;
            }
            const last_id = c.mysql_stmt_insert_id(insert_color_stmt);
            _ = c.mysql_stmt_reset(insert_color_stmt);

            inline for (cat_names) |cat_name| {
                var cat_binds = [_]c.MYSQL_BIND{ std.mem.zeroes(c.MYSQL_BIND), std.mem.zeroes(c.MYSQL_BIND) };
                cat_binds[0].buffer_type = c.MYSQL_TYPE_STRING;
                cat_binds[0].buffer_length = cat_name.len;
                cat_binds[0].buffer = @constCast(@ptrCast(cat_name.ptr));

                cat_binds[1].buffer_type = c.MYSQL_TYPE_LONG;
                cat_binds[1].length = (@as(c_ulong, 1));
                cat_binds[1].buffer = @constCast(@ptrCast(&last_id));

                if (c.mysql_stmt_bind_param(insert_cat_stmt, &cat_binds) != 0) {
                    print("Bind cat param failed: {s}\n", .{c.mysql_error(self.conn)});
                    return error.bindParamError;
                }
                if (c.mysql_stmt_execute(insert_cat_stmt) != 0) {
                    print("Exec cat stmt failed: {s}\n", .{c.mysql_error(self.conn)});
                    return error.execStmtError;
                }

                _ = c.mysql_stmt_reset(insert_cat_stmt);
            }
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const version = c.mysql_get_client_version();
    print("MySQL client version is {}\n", .{version});

    const db = try DB.init(allocator, .{
        .database = "testdb",
        .host = "172.17.0.1",
        .user = "root",
        .password = "my-secret-pw",
    });
    defer db.deinit();

    try db.execute(
        \\ DROP TABLE IF EXISTS cat_colors;
        \\ DROP TABLE IF EXISTS cats;
        \\ CREATE TABLE IF NOT EXISTS cat_colors (
        \\  id INT AUTO_INCREMENT PRIMARY KEY,
        \\  name VARCHAR(255) NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS cats (
        \\  id INT AUTO_INCREMENT PRIMARY KEY,
        \\  name VARCHAR(255) NOT NULL,
        \\  color_id INT NOT NULL
        \\)
    );
    // Since we use multi-statement, we need to consume all results.
    // Otherwise we will get following error when we execute next query.
    // Commands out of sync; you can't run this command now
    //
    // https://dev.mysql.com/doc/c-api/8.0/en/mysql-next-result.html
    while (c.mysql_next_result(db.conn) == 0) {
        const res = c.mysql_store_result(db.conn);
        c.mysql_free_result(res);
    }

    try db.insertTable();
    try db.queryTable();
}
