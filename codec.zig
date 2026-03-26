const std = @import("std");
const types = @import("types.zig");
const transport = @import("transport.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const Value = types.Value;

pub const Codec = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write: fn (ctx: *anyopaque, allocator: std.mem.Allocator, stream: transport.Stream, instr: ProxyInstruction) anyerror!void,
        read: fn (ctx: *anyopaque, allocator: std.mem.Allocator, stream: transport.Stream) anyerror!ProxyInstruction,
    };

    pub fn write(self: Codec, allocator: std.mem.Allocator, stream: transport.Stream, instr: ProxyInstruction) !void {
        return self.vtable.write(self.ctx, allocator, stream, instr);
    }

    pub fn read(self: Codec, allocator: std.mem.Allocator, stream: transport.Stream) !ProxyInstruction {
        return self.vtable.read(self.ctx, allocator, stream);
    }
};

pub const JsonCodec = struct {
    pub fn codec(self: *JsonCodec) Codec {
        return Codec{ .ctx = self, .vtable = &vtable };
    }

    fn write(_: *anyopaque, allocator: std.mem.Allocator, stream: transport.Stream, instr: ProxyInstruction) anyerror!void {
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();

        const json_val = try instruction_to_json(allocator, instr);
        defer json_val.deinit();

        try std.json.stringify(json_val, .{}, list.writer());
        const payload = list.items;

        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @as(u32, @intCast(payload.len)), .big);
        try stream.write(&len_buf);
        if (payload.len > 0) {
            try stream.write(payload);
        }
    }

    fn read(_: *anyopaque, allocator: std.mem.Allocator, stream: transport.Stream) anyerror!ProxyInstruction {
        var len_buf: [4]u8 = undefined;
        try read_exact(stream, &len_buf);
        const len = std.mem.readInt(u32, &len_buf, .big);
        const payload = try allocator.alloc(u8, len);
        defer allocator.free(payload);
        if (len > 0) {
            try read_exact(stream, payload);
        }

        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
        defer parsed.deinit();
        return instruction_from_json(allocator, parsed.value);
    }

    fn read_exact(stream: transport.Stream, buf: []u8) !void {
        var filled: usize = 0;
        while (filled < buf.len) {
            const n = try stream.read(buf[filled..]);
            if (n == 0) return error.EndOfStream;
            filled += n;
        }
    }

    const vtable = Codec.VTable{ .write = write, .read = read };
};

fn instruction_to_json(allocator: std.mem.Allocator, instr: ProxyInstruction) !std.json.Value {
    var object = std.json.ObjectMap.init(allocator);
    if (instr.id) |id| {
        try object.put("id", std.json.Value{ .string = try allocator.dupe(u8, id) });
    }
    try object.put("kind", std.json.Value{ .integer = @as(i64, instr.kind) });
    try object.put("data", try value_to_json(allocator, instr.data));
    if (instr.metadata) |meta| {
        try object.put("metadata", try value_to_json(allocator, meta));
    }
    return std.json.Value{ .object = object };
}

fn instruction_from_json(allocator: std.mem.Allocator, value: std.json.Value) !ProxyInstruction {
    if (value != .object) return error.InvalidInstruction;
    const obj = value.object;

    var kind_val: ?u32 = null;
    var id_val: ?[]const u8 = null;
    var data_val: ?Value = null;
    var meta_val: ?Value = null;

    if (obj.get("kind")) |k| {
        switch (k) {
            .integer => |i| kind_val = std.math.cast(u32, i),
            .float => |f| kind_val = std.math.cast(u32, @as(i64, @intFromFloat(f))),
            else => {},
        }
    }
    if (obj.get("id")) |idv| {
        if (idv == .string) id_val = try allocator.dupe(u8, idv.string);
    }
    if (obj.get("data")) |dv| {
        data_val = try value_from_json(allocator, dv);
    }
    if (obj.get("metadata")) |mv| {
        meta_val = try value_from_json(allocator, mv);
    }

    if (kind_val == null or data_val == null) return error.InvalidInstruction;

    return ProxyInstruction{
        .id = id_val,
        .kind = kind_val.?,
        .data = data_val.?,
        .metadata = meta_val,
    };
}

fn value_to_json(allocator: std.mem.Allocator, value: Value) !std.json.Value {
    return switch (value) {
        .null, .undefined => std.json.Value{ .null = {} },
        .boolean => |b| std.json.Value{ .bool = b },
        .int => |i| std.json.Value{ .integer = i },
        .uint => |u| std.json.Value{ .integer = std.math.lossyCast(i64, u) },
        .float => |f| std.json.Value{ .float = f },
        .string => |s| std.json.Value{ .string = try allocator.dupe(u8, s) },
        .binary => |b| blk: {
            var arr = std.json.Array.init(allocator);
            for (b) |byte| try arr.append(std.json.Value{ .integer = byte });
            break :blk std.json.Value{ .array = arr };
        },
        .reference => |s| std.json.Value{ .string = try allocator.dupe(u8, s) },
        .array => |items| blk: {
            var arr = std.json.Array.init(allocator);
            for (items) |item| try arr.append(try value_to_json(allocator, item));
            break :blk std.json.Value{ .array = arr };
        },
        .map => |items| blk: {
            var obj = std.json.ObjectMap.init(allocator);
            for (items) |entry| {
                try obj.put(entry.key, try value_to_json(allocator, entry.value));
            }
            break :blk std.json.Value{ .object = obj };
        },
    };
}

fn value_from_json(allocator: std.mem.Allocator, value: std.json.Value) !Value {
    return switch (value) {
        .null => Value.null,
        .bool => |b| Value{ .boolean = b },
        .integer => |i| Value{ .int = i },
        .float => |f| Value{ .float = f },
        .string => |s| Value{ .string = try allocator.dupe(u8, s) },
        .array => |arr| blk: {
            var out = try allocator.alloc(Value, arr.items.len);
            for (arr.items, 0..) |item, i| out[i] = try value_from_json(allocator, item);
            break :blk Value{ .array = out };
        },
        .object => |obj| blk: {
            var out = try allocator.alloc(types.MapEntry, obj.count());
            var it = obj.iterator();
            var idx: usize = 0;
            while (it.next()) |entry| : (idx += 1) {
                out[idx] = .{ .key = try allocator.dupe(u8, entry.key_ptr.*), .value = try value_from_json(allocator, entry.value_ptr.*) };
            }
            break :blk Value{ .map = out[0..idx] };
        },
    };
}
