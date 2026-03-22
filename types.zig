const std = @import("std");

pub const ValueKind = enum(u32) {
    function = 0x9ed64249,
    array = 0x8a58ad26,
    string = 0x17c16538,
    number = 0x1bd670a0,
    boolean = 0x65f46ebf,
    symbol = 0xf3fb51d1,
    object = 0xb8c60cba,
    bigint = 0x8a67a5ca,
    unknown = 0x9b759fb9,
    null = 0x77074ba4,
    undefined = 0x9b61ad43,
    reference = 0x5a1b3c4d,
};

pub const InstructionKind = enum(u32) {
    local = 0x9c436708,
    get = 0x540ca757,
    set = 0xc6270703,
    apply = 0x24bc4a3b,
    construct = 0x40c09172,
    execute = 0xa01e3d98,
    throw = 0x7a78762f,
    ret = 0x85ee37bf,
    next = 0x5cb68de8,
    release = 0x1a2b3c4d,
};

pub const ProxyError = struct {
    message: []const u8,
    cause: ?*ProxyError = null,
};

pub const ProxyInstruction = struct {
    id: ?[]const u8 = null,
    kind: u32,
    data: Value,
    metadata: ?Value = null,
};

pub const MapEntry = struct {
    key: []const u8,
    value: Value,
};

pub const Value = union(enum) {
    null,
    undefined,
    boolean: bool,
    int: i64,
    uint: u64,
    float: f64,
    string: []const u8,
    binary: []const u8,
    array: []Value,
    map: []MapEntry,
    reference: []const u8,

    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => allocator.free(self.string),
            .binary => allocator.free(self.binary),
            .array => {
                for (self.array) |*item| item.deinit(allocator);
                allocator.free(self.array);
            },
            .map => {
                for (self.map) |*entry| {
                    allocator.free(entry.key);
                    entry.value.deinit(allocator);
                }
                allocator.free(self.map);
            },
            .reference => allocator.free(self.reference),
            else => {},
        }
    }

    pub fn clone(self: Value, allocator: std.mem.Allocator) !Value {
        return switch (self) {
            .null => .null,
            .undefined => .undefined,
            .boolean => |v| Value{ .boolean = v },
            .int => |v| Value{ .int = v },
            .uint => |v| Value{ .uint = v },
            .float => |v| Value{ .float = v },
            .string => |v| Value{ .string = try allocator.dupe(u8, v) },
            .binary => |v| Value{ .binary = try allocator.dupe(u8, v) },
            .reference => |v| Value{ .reference = try allocator.dupe(u8, v) },
            .array => |items| blk: {
                var out = try allocator.alloc(Value, items.len);
                for (items, 0..) |item, i| out[i] = try item.clone(allocator);
                break :blk Value{ .array = out };
            },
            .map => |items| blk: {
                var out = try allocator.alloc(MapEntry, items.len);
                for (items, 0..) |entry, i| {
                    out[i] = MapEntry{ .key = try allocator.dupe(u8, entry.key), .value = try entry.value.clone(allocator) };
                }
                break :blk Value{ .map = out };
            },
        };
    }
};

pub fn is_primitive(value: Value) bool {
    return switch (value) {
        .null, .undefined, .boolean, .int, .uint, .float, .string, .binary => true,
        else => false,
    };
}

pub fn value_kind_of(value: Value) ValueKind {
    return switch (value) {
        .null => .null,
        .undefined => .undefined,
        .boolean => .boolean,
        .int, .uint, .float => .number,
        .string => .string,
        .binary => .unknown,
        .array => .array,
        .map => .object,
        .reference => .reference,
    };
}
