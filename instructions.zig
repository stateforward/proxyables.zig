const std = @import("std");
const types = @import("types.zig");
const muid = @import("muid.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const InstructionKind = types.InstructionKind;
pub const Value = types.Value;
pub const ValueKind = types.ValueKind;
pub const ProxyError = types.ProxyError;

pub fn create_instruction_unsafe(allocator: std.mem.Allocator, kind: InstructionKind, data: Value) !ProxyInstruction {
    return ProxyInstruction{
        .id = try muid.make(allocator),
        .kind = @intFromEnum(kind),
        .data = data,
        .metadata = null,
    };
}

pub fn create_value_instruction(allocator: std.mem.Allocator, value: Value) !ProxyInstruction {
    const kind = types.value_kind_of(value);
    return ProxyInstruction{
        .id = try muid.make(allocator),
        .kind = @intFromEnum(kind),
        .data = value,
        .metadata = null,
    };
}

pub fn create_throw_instruction(allocator: std.mem.Allocator, err: ProxyError) !ProxyInstruction {
    const err_value = try proxy_error_to_value(allocator, err);
    return ProxyInstruction{
        .id = try muid.make(allocator),
        .kind = @intFromEnum(InstructionKind.throw),
        .data = err_value,
        .metadata = null,
    };
}

pub fn create_get_instruction(allocator: std.mem.Allocator, key: []const u8) !ProxyInstruction {
    var list = try allocator.alloc(Value, 1);
    list[0] = Value{ .string = try allocator.dupe(u8, key) };
    return create_instruction_unsafe(allocator, .get, Value{ .array = list });
}

pub fn create_apply_instruction(allocator: std.mem.Allocator, args: []const Value) !ProxyInstruction {
    var copied = try allocator.alloc(Value, args.len);
    for (args, 0..) |arg, i| copied[i] = try arg.clone(allocator);
    return create_instruction_unsafe(allocator, .apply, Value{ .array = copied });
}

pub fn create_construct_instruction(allocator: std.mem.Allocator, args: []const Value) !ProxyInstruction {
    var copied = try allocator.alloc(Value, args.len);
    for (args, 0..) |arg, i| copied[i] = try arg.clone(allocator);
    return create_instruction_unsafe(allocator, .construct, Value{ .array = copied });
}

pub fn create_return_instruction(allocator: std.mem.Allocator, value: Value) !ProxyInstruction {
    var value_instr = try create_value_instruction(allocator, value);
    const value_payload = try instruction_to_value(allocator, value_instr);
    if (value_instr.id) |id| allocator.free(id);
    value_instr.data.deinit(allocator);
    if (value_instr.metadata) |*meta| meta.deinit(allocator);
    return ProxyInstruction{
        .id = try muid.make(allocator),
        .kind = @intFromEnum(InstructionKind.@"return"),
        .data = value_payload,
        .metadata = null,
    };
}

pub fn create_execute_instruction(allocator: std.mem.Allocator, instructions: []const ProxyInstruction) !ProxyInstruction {
    var list = try allocator.alloc(Value, instructions.len);
    for (instructions, 0..) |instr, i| {
        list[i] = try instruction_to_value(allocator, instr);
    }
    return create_instruction_unsafe(allocator, .execute, Value{ .array = list });
}

pub fn create_release_instruction(allocator: std.mem.Allocator, ref_id: []const u8) !ProxyInstruction {
    var list = try allocator.alloc(Value, 1);
    list[0] = Value{ .string = try allocator.dupe(u8, ref_id) };
    return ProxyInstruction{
        .id = try muid.make(allocator),
        .kind = @intFromEnum(InstructionKind.release),
        .data = Value{ .array = list },
        .metadata = null,
    };
}

pub fn instruction_to_value(allocator: std.mem.Allocator, instr: ProxyInstruction) !Value {
    const entry_count: usize = 2 +
        @as(usize, @intFromBool(instr.id != null)) +
        @as(usize, @intFromBool(instr.metadata != null));
    var entries = try allocator.alloc(types.MapEntry, entry_count);
    var count: usize = 0;

    if (instr.id) |id| {
        entries[count] = .{ .key = try allocator.dupe(u8, "id"), .value = Value{ .string = try allocator.dupe(u8, id) } };
        count += 1;
    }
    entries[count] = .{ .key = try allocator.dupe(u8, "kind"), .value = Value{ .uint = instr.kind } };
    count += 1;
    entries[count] = .{ .key = try allocator.dupe(u8, "data"), .value = try instr.data.clone(allocator) };
    count += 1;
    if (instr.metadata) |meta| {
        entries[count] = .{ .key = try allocator.dupe(u8, "metadata"), .value = try meta.clone(allocator) };
        count += 1;
    }

    return Value{ .map = entries[0..count] };
}

pub fn proxy_error_to_value(allocator: std.mem.Allocator, err: ProxyError) !Value {
    var entries = try allocator.alloc(types.MapEntry, if (err.cause == null) 1 else 2);
    entries[0] = .{ .key = try allocator.dupe(u8, "message"), .value = Value{ .string = try allocator.dupe(u8, err.message) } };
    if (err.cause) |cause| {
        entries[1] = .{ .key = try allocator.dupe(u8, "cause"), .value = try proxy_error_to_value(allocator, cause.*) };
    }
    return Value{ .map = entries };
}

test "dsl constants match shared contract" {
    try std.testing.expectEqual(@as(u32, 0x5a1b3c4d), @intFromEnum(ValueKind.reference));
    try std.testing.expectEqual(@as(u32, 0xa01e3d98), @intFromEnum(InstructionKind.execute));
    try std.testing.expectEqual(@as(u32, 0x1a2b3c4d), @intFromEnum(InstructionKind.release));
}

test "dsl instruction shapes are canonical" {
    const allocator = std.testing.allocator;

    var get = try create_get_instruction(allocator, "key");
    defer {
        if (get.id) |id| allocator.free(id);
        get.data.deinit(allocator);
    }
    try std.testing.expectEqual(@intFromEnum(InstructionKind.get), get.kind);

    const args = [_]Value{ Value{ .int = 1 }, Value{ .int = 2 } };
    var apply = try create_apply_instruction(allocator, &args);
    defer {
        if (apply.id) |id| allocator.free(id);
        apply.data.deinit(allocator);
    }
    try std.testing.expectEqual(@intFromEnum(InstructionKind.apply), apply.kind);

    var construct = try create_construct_instruction(allocator, &args);
    defer {
        if (construct.id) |id| allocator.free(id);
        construct.data.deinit(allocator);
    }
    try std.testing.expectEqual(@intFromEnum(InstructionKind.construct), construct.kind);

    var release = try create_release_instruction(allocator, "ref-1");
    defer {
        if (release.id) |id| allocator.free(id);
        release.data.deinit(allocator);
    }
    try std.testing.expectEqual(@intFromEnum(InstructionKind.release), release.kind);

    const execute_fixture = ProxyInstruction{
        .id = null,
        .kind = @intFromEnum(InstructionKind.get),
        .data = .null,
        .metadata = null,
    };
    var exec = try create_execute_instruction(allocator, &[_]ProxyInstruction{execute_fixture});
    defer {
        if (exec.id) |id| allocator.free(id);
        exec.data.deinit(allocator);
    }
    try std.testing.expectEqual(@intFromEnum(InstructionKind.execute), exec.kind);
}
