const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

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

pub const Row = struct {
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
