const std = @import("std");
const types = @import("types.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const Value = types.Value;
pub const MapEntry = types.MapEntry;
pub const ProxyError = types.ProxyError;

pub fn parse_instruction_list(allocator: std.mem.Allocator, data: Value) ![]ProxyInstruction {
    if (data != .array) return error.InvalidInstructionList;
    var out = try allocator.alloc(ProxyInstruction, data.array.len);
    for (data.array, 0..) |item, i| {
        out[i] = try parse_instruction_value(allocator, item);
    }
    return out;
}

pub fn parse_instruction_value(allocator: std.mem.Allocator, value: Value) !ProxyInstruction {
    if (value != .map) return error.InvalidInstruction;
    const map = value.map;

    const kind_val = map_get(map, "kind") orelse return error.InvalidInstruction;
    const data_val = map_get(map, "data") orelse return error.InvalidInstruction;

    const kind = try parse_u32(allocator, kind_val);

    var id_val: ?[]const u8 = null;
    if (map_get(map, "id")) |id_value| {
        if (id_value == .string) id_val = try allocator.dupe(u8, id_value.string);
    }

    var metadata_val: ?Value = null;
    if (map_get(map, "metadata")) |meta| {
        metadata_val = try meta.clone(allocator);
    }

    return ProxyInstruction{
        .id = id_val,
        .kind = kind,
        .data = try data_val.clone(allocator),
        .metadata = metadata_val,
    };
}

pub fn parse_get_key(allocator: std.mem.Allocator, data: Value) ![]const u8 {
    if (data != .array or data.array.len == 0) return error.InvalidGetData;
    const key = data.array[0];
    return switch (key) {
        .string => |s| try allocator.dupe(u8, s),
        .int => |i| try std.fmt.allocPrint(allocator, "{}", .{i}),
        .uint => |u| try std.fmt.allocPrint(allocator, "{}", .{u}),
        .float => |f| try std.fmt.allocPrint(allocator, "{}", .{f}),
        else => return error.InvalidGetData,
    };
}

pub fn parse_args(allocator: std.mem.Allocator, data: Value) ![]Value {
    if (data == .array) {
        var out = try allocator.alloc(Value, data.array.len);
        for (data.array, 0..) |item, i| out[i] = try item.clone(allocator);
        return out;
    }
    if (data == .null or data == .undefined) {
        return allocator.alloc(Value, 0);
    }
    return error.InvalidApplyData;
}

pub fn parse_release_id(allocator: std.mem.Allocator, data: Value) ?[]const u8 {
    if (data != .array or data.array.len == 0) return null;
    const id_val = data.array[0];
    if (id_val == .string) {
        return allocator.dupe(u8, id_val.string) catch null;
    }
    return null;
}

pub fn parse_proxy_error(allocator: std.mem.Allocator, data: Value) ProxyError {
    if (data != .map) {
        return ProxyError{ .message = "unknown error" };
    }
    const message_val = map_get(data.map, "message");
    var message: []const u8 = "unknown error";
    if (message_val) |mv| {
        if (mv == .string) message = allocator.dupe(u8, mv.string) catch mv.string;
    }

    var cause: ?*ProxyError = null;
    if (map_get(data.map, "cause")) |cv| {
        if (cv == .map) {
            const cause_ptr = allocator.create(ProxyError) catch null;
            if (cause_ptr) |ptr| {
                ptr.* = parse_proxy_error(allocator, cv);
                cause = ptr;
            }
        }
    }
    return ProxyError{ .message = message, .cause = cause };
}

fn map_get(map: []MapEntry, key: []const u8) ?Value {
    for (map) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

fn parse_u32(allocator: std.mem.Allocator, value: Value) !u32 {
    _ = allocator;
    return switch (value) {
        .uint => |u| @intCast(u32, u),
        .int => |i| @intCast(u32, i),
        .float => |f| @intCast(u32, @intFromFloat(i64, f)),
        .string => |s| blk: {
            const parsed = try std.fmt.parseInt(u32, s, 10);
            break :blk parsed;
        },
        else => error.InvalidKind,
    };
}
