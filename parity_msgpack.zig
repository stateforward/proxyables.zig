const std = @import("std");
const codec_mod = @import("codec.zig");
const transport = @import("transport.zig");
const types = @import("types.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const Value = types.Value;
pub const MapEntry = types.MapEntry;

const DecodeError = error{ EndOfStream, IncompleteData, InvalidData, UnsupportedType };
const DecodeValueError = DecodeError || std.mem.Allocator.Error;

pub fn codec() codec_mod.Codec {
    return singleton.codec();
}

const singleton = CodecImpl{};

const CodecImpl = struct {
    fn codec(self: *const CodecImpl) codec_mod.Codec {
        return .{ .ctx = @constCast(self), .vtable = &vtable };
    }

    fn write(_: *anyopaque, allocator: std.mem.Allocator, stream: transport.Stream, instruction: ProxyInstruction) !void {
        var bytes = std.array_list.Managed(u8).init(allocator);
        defer bytes.deinit();

        try writeInstruction(bytes.writer(), instruction);
        try stream.write(bytes.items);
    }

    fn read(_: *anyopaque, allocator: std.mem.Allocator, stream: transport.Stream) !ProxyInstruction {
        var buffer = std.array_list.Managed(u8).init(allocator);
        defer buffer.deinit();

        var chunk: [1024]u8 = undefined;
        while (true) {
            if (buffer.items.len > 0) {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();
                var cursor: usize = 0;
                const parsed = decodeInstruction(arena.allocator(), buffer.items, &cursor) catch |err| switch (err) {
                    DecodeError.IncompleteData => null,
                    else => return err,
                };
                if (parsed) |instruction| {
                    return cloneInstruction(allocator, instruction);
                }
            }

            const read_count = try stream.read(&chunk);
            if (read_count == 0) return DecodeError.EndOfStream;
            try buffer.appendSlice(chunk[0..read_count]);
        }
    }

    const vtable = codec_mod.Codec.VTable{
        .write = write,
        .read = read,
    };
};

fn cloneInstruction(allocator: std.mem.Allocator, instruction: ProxyInstruction) !ProxyInstruction {
    return .{
        .id = if (instruction.id) |id| try allocator.dupe(u8, id) else null,
        .kind = instruction.kind,
        .data = try instruction.data.clone(allocator),
        .metadata = if (instruction.metadata) |metadata| try metadata.clone(allocator) else null,
    };
}

fn writeInstruction(writer: anytype, instruction: ProxyInstruction) !void {
    var fields: usize = 2;
    if (instruction.id != null) fields += 1;
    if (instruction.metadata != null) fields += 1;

    try writeMapHeader(writer, fields);

    try writeString(writer, "kind");
    try writeU64(writer, instruction.kind);
    try writeString(writer, "data");
    try writeValue(writer, instruction.data);

    if (instruction.id) |id| {
        try writeString(writer, "id");
        try writeString(writer, id);
    }

    if (instruction.metadata) |metadata| {
        try writeString(writer, "metadata");
        try writeValue(writer, metadata);
    }
}

fn writeValue(writer: anytype, value: Value) !void {
    switch (value) {
        .null => try writer.writeByte(0xc0),
        .undefined => try writer.writeByte(0xc0),
        .boolean => |flag| try writer.writeByte(if (flag) 0xc3 else 0xc2),
        .int => |number| try writeI64(writer, number),
        .uint => |number| try writeU64(writer, number),
        .float => |number| {
            try writer.writeByte(0xcb);
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, @bitCast(number), .big);
            try writer.writeAll(&bytes);
        },
        .string => |text| try writeString(writer, text),
        .binary => |data| try writeBinary(writer, data),
        .reference => |id| try writeString(writer, id),
        .array => |items| {
            try writeArrayHeader(writer, items.len);
            for (items) |item| {
                try writeValue(writer, item);
            }
        },
        .map => |entries| {
            try writeMapHeader(writer, entries.len);
            for (entries) |entry| {
                try writeString(writer, entry.key);
                try writeValue(writer, entry.value);
            }
        },
    }
}

fn writeMapHeader(writer: anytype, count: usize) !void {
    if (count < 16) {
        try writer.writeByte(0x80 | @as(u8, @intCast(count)));
    } else if (count < 65536) {
        try writer.writeByte(0xde);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, @intCast(count), .big);
        try writer.writeAll(&bytes);
    } else {
        try writer.writeByte(0xdf);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(count), .big);
        try writer.writeAll(&bytes);
    }
}

fn writeArrayHeader(writer: anytype, count: usize) !void {
    if (count < 16) {
        try writer.writeByte(0x90 | @as(u8, @intCast(count)));
    } else if (count < 65536) {
        try writer.writeByte(0xdc);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, @intCast(count), .big);
        try writer.writeAll(&bytes);
    } else {
        try writer.writeByte(0xdd);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(count), .big);
        try writer.writeAll(&bytes);
    }
}

fn writeString(writer: anytype, text: []const u8) !void {
    if (text.len < 32) {
        try writer.writeByte(0xa0 | @as(u8, @intCast(text.len)));
    } else if (text.len < 256) {
        try writer.writeByte(0xd9);
        try writer.writeByte(@intCast(text.len));
    } else if (text.len < 65536) {
        try writer.writeByte(0xda);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, @intCast(text.len), .big);
        try writer.writeAll(&bytes);
    } else {
        try writer.writeByte(0xdb);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(text.len), .big);
        try writer.writeAll(&bytes);
    }
    try writer.writeAll(text);
}

fn writeBinary(writer: anytype, bytes_in: []const u8) !void {
    if (bytes_in.len < 256) {
        try writer.writeByte(0xc4);
        try writer.writeByte(@intCast(bytes_in.len));
    } else if (bytes_in.len < 65536) {
        try writer.writeByte(0xc5);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, @intCast(bytes_in.len), .big);
        try writer.writeAll(&bytes);
    } else {
        try writer.writeByte(0xc6);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(bytes_in.len), .big);
        try writer.writeAll(&bytes);
    }
    try writer.writeAll(bytes_in);
}

fn writeU64(writer: anytype, value: u64) !void {
    if (value <= 0x7f) {
        try writer.writeByte(@intCast(value));
    } else if (value <= std.math.maxInt(u8)) {
        try writer.writeByte(0xcc);
        try writer.writeByte(@intCast(value));
    } else if (value <= std.math.maxInt(u16)) {
        try writer.writeByte(0xcd);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, @intCast(value), .big);
        try writer.writeAll(&bytes);
    } else if (value <= std.math.maxInt(u32)) {
        try writer.writeByte(0xce);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(value), .big);
        try writer.writeAll(&bytes);
    } else {
        try writer.writeByte(0xcf);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .big);
        try writer.writeAll(&bytes);
    }
}

fn writeI64(writer: anytype, value: i64) !void {
    if (value >= 0) return writeU64(writer, @intCast(value));
    if (value >= -32) {
        const encoded: i8 = @intCast(value);
        try writer.writeByte(@bitCast(encoded));
    } else if (value >= std.math.minInt(i8)) {
        try writer.writeByte(0xd0);
        const encoded: i8 = @intCast(value);
        try writer.writeByte(@bitCast(encoded));
    } else if (value >= std.math.minInt(i16)) {
        try writer.writeByte(0xd1);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(i16, &bytes, @intCast(value), .big);
        try writer.writeAll(&bytes);
    } else if (value >= std.math.minInt(i32)) {
        try writer.writeByte(0xd2);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, @intCast(value), .big);
        try writer.writeAll(&bytes);
    } else {
        try writer.writeByte(0xd3);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, value, .big);
        try writer.writeAll(&bytes);
    }
}

fn decodeInstruction(allocator: std.mem.Allocator, bytes: []const u8, cursor: *usize) DecodeValueError!ProxyInstruction {
    const root = try decodeValue(allocator, bytes, cursor);
    if (root != .map) return DecodeError.InvalidData;

    var id_value: ?[]const u8 = null;
    var kind_value: ?u32 = null;
    var data_value: ?Value = null;
    var metadata_value: ?Value = null;

    for (root.map) |entry| {
        if (std.mem.eql(u8, entry.key, "id")) {
            if (entry.value == .string) {
                id_value = try allocator.dupe(u8, entry.value.string);
            }
        } else if (std.mem.eql(u8, entry.key, "kind")) {
            kind_value = switch (entry.value) {
                .uint => |number| @intCast(number),
                .int => |number| @intCast(number),
                else => return DecodeError.InvalidData,
            };
        } else if (std.mem.eql(u8, entry.key, "data")) {
            data_value = try entry.value.clone(allocator);
        } else if (std.mem.eql(u8, entry.key, "metadata")) {
            metadata_value = try entry.value.clone(allocator);
        }
    }

    if (kind_value == null or data_value == null) return DecodeError.InvalidData;
    return .{
        .id = id_value,
        .kind = kind_value.?,
        .data = data_value.?,
        .metadata = metadata_value,
    };
}

fn decodeValue(allocator: std.mem.Allocator, bytes: []const u8, cursor: *usize) DecodeValueError!Value {
    const token = try readByte(bytes, cursor);

    return switch (token) {
        0xc0 => Value.null,
        0xc2 => Value{ .boolean = false },
        0xc3 => Value{ .boolean = true },
        0xcc => Value{ .uint = try readU8(bytes, cursor) },
        0xcd => Value{ .uint = try readU16(bytes, cursor) },
        0xce => Value{ .uint = try readU32(bytes, cursor) },
        0xcf => Value{ .uint = try readU64(bytes, cursor) },
        0xd0 => Value{ .int = try readI8(bytes, cursor) },
        0xd1 => Value{ .int = try readI16(bytes, cursor) },
        0xd2 => Value{ .int = try readI32(bytes, cursor) },
        0xd3 => Value{ .int = try readI64(bytes, cursor) },
        0xca => blk: {
            const bits = try readU32(bytes, cursor);
            break :blk Value{ .float = @floatCast(@as(f32, @bitCast(bits))) };
        },
        0xcb => blk: {
            const bits = try readU64(bytes, cursor);
            break :blk Value{ .float = @as(f64, @bitCast(bits)) };
        },
        0xc4 => Value{ .binary = try takeBytes(allocator, bytes, cursor, try readU8(bytes, cursor)) },
        0xc5 => Value{ .binary = try takeBytes(allocator, bytes, cursor, try readU16(bytes, cursor)) },
        0xc6 => Value{ .binary = try takeBytes(allocator, bytes, cursor, try readU32(bytes, cursor)) },
        0xd9 => Value{ .string = try takeBytes(allocator, bytes, cursor, try readU8(bytes, cursor)) },
        0xda => Value{ .string = try takeBytes(allocator, bytes, cursor, try readU16(bytes, cursor)) },
        0xdb => Value{ .string = try takeBytes(allocator, bytes, cursor, try readU32(bytes, cursor)) },
        0xdc => try decodeArray(allocator, bytes, cursor, try readU16(bytes, cursor)),
        0xdd => try decodeArray(allocator, bytes, cursor, try readU32(bytes, cursor)),
        0xde => try decodeMap(allocator, bytes, cursor, try readU16(bytes, cursor)),
        0xdf => try decodeMap(allocator, bytes, cursor, try readU32(bytes, cursor)),
        0x90...0x9f => try decodeArray(allocator, bytes, cursor, token & 0x0f),
        0x80...0x8f => try decodeMap(allocator, bytes, cursor, token & 0x0f),
        0xa0...0xbf => Value{ .string = try takeBytes(allocator, bytes, cursor, token & 0x1f) },
        0xe0...0xff => Value{ .int = @as(i8, @bitCast(token)) },
        else => if (token <= 0x7f) Value{ .uint = token } else DecodeError.UnsupportedType,
    };
}

fn decodeArray(allocator: std.mem.Allocator, bytes: []const u8, cursor: *usize, count_raw: anytype) DecodeValueError!Value {
    const count: usize = @intCast(count_raw);
    const out = try allocator.alloc(Value, count);
    for (0..count) |index| {
        out[index] = try decodeValue(allocator, bytes, cursor);
    }
    return Value{ .array = out };
}

fn decodeMap(allocator: std.mem.Allocator, bytes: []const u8, cursor: *usize, count_raw: anytype) DecodeValueError!Value {
    const count: usize = @intCast(count_raw);
    const out = try allocator.alloc(MapEntry, count);
    for (0..count) |index| {
        const key_value = try decodeValue(allocator, bytes, cursor);
        if (key_value != .string) return DecodeError.InvalidData;
        out[index] = .{
            .key = key_value.string,
            .value = try decodeValue(allocator, bytes, cursor),
        };
    }
    return Value{ .map = out };
}

fn takeBytes(allocator: std.mem.Allocator, bytes: []const u8, cursor: *usize, count_raw: anytype) DecodeValueError![]u8 {
    const count: usize = @intCast(count_raw);
    if (cursor.* + count > bytes.len) return DecodeError.IncompleteData;
    defer cursor.* += count;
    return allocator.dupe(u8, bytes[cursor.* .. cursor.* + count]);
}

fn readByte(bytes: []const u8, cursor: *usize) !u8 {
    if (cursor.* >= bytes.len) return DecodeError.IncompleteData;
    defer cursor.* += 1;
    return bytes[cursor.*];
}

fn readU8(bytes: []const u8, cursor: *usize) !u8 {
    return readByte(bytes, cursor);
}

fn readI8(bytes: []const u8, cursor: *usize) !i8 {
    return @bitCast(try readByte(bytes, cursor));
}

fn readU16(bytes: []const u8, cursor: *usize) !u16 {
    if (cursor.* + 2 > bytes.len) return DecodeError.IncompleteData;
    defer cursor.* += 2;
    return (@as(u16, bytes[cursor.*]) << 8) | @as(u16, bytes[cursor.* + 1]);
}

fn readI16(bytes: []const u8, cursor: *usize) !i16 {
    return @bitCast(try readU16(bytes, cursor));
}

fn readU32(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* + 4 > bytes.len) return DecodeError.IncompleteData;
    defer cursor.* += 4;
    return (@as(u32, bytes[cursor.*]) << 24) |
        (@as(u32, bytes[cursor.* + 1]) << 16) |
        (@as(u32, bytes[cursor.* + 2]) << 8) |
        @as(u32, bytes[cursor.* + 3]);
}

fn readI32(bytes: []const u8, cursor: *usize) !i32 {
    return @bitCast(try readU32(bytes, cursor));
}

fn readU64(bytes: []const u8, cursor: *usize) !u64 {
    if (cursor.* + 8 > bytes.len) return DecodeError.IncompleteData;
    defer cursor.* += 8;
    return (@as(u64, bytes[cursor.*]) << 56) |
        (@as(u64, bytes[cursor.* + 1]) << 48) |
        (@as(u64, bytes[cursor.* + 2]) << 40) |
        (@as(u64, bytes[cursor.* + 3]) << 32) |
        (@as(u64, bytes[cursor.* + 4]) << 24) |
        (@as(u64, bytes[cursor.* + 5]) << 16) |
        (@as(u64, bytes[cursor.* + 6]) << 8) |
        @as(u64, bytes[cursor.* + 7]);
}

fn readI64(bytes: []const u8, cursor: *usize) !i64 {
    return @bitCast(try readU64(bytes, cursor));
}
