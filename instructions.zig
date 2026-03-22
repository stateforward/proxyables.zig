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

pub fn create_return_instruction(allocator: std.mem.Allocator, value: Value) !ProxyInstruction {
    var value_instr = try create_value_instruction(allocator, value);
    const value_payload = try instruction_to_value(allocator, value_instr);
    if (value_instr.id) |id| allocator.free(id);
    value_instr.data.deinit(allocator);
    if (value_instr.metadata) |*meta| meta.deinit(allocator);
    return ProxyInstruction{
        .id = try muid.make(allocator),
        .kind = @intFromEnum(InstructionKind.ret),
        .data = value_payload,
        .metadata = null,
    };
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
    var entries = try allocator.alloc(types.MapEntry, 4);
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
