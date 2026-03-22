const std = @import("std");
const types = @import("types.zig");
const helpers = @import("helpers.zig");
const instructions_mod = @import("instructions.zig");
const registry_mod = @import("registry.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const InstructionKind = types.InstructionKind;
pub const ValueKind = types.ValueKind;
pub const Value = types.Value;
pub const ProxyError = types.ProxyError;
pub const ProxyTarget = registry_mod.ProxyTarget;

pub fn evaluate_instructions(
    allocator: std.mem.Allocator,
    instructions: []const ProxyInstruction,
    registry: *registry_mod.ObjectRegistry,
    root: ?ProxyTarget,
) !ProxyInstruction {
    var stack = std.ArrayList(ProxyInstruction).init(allocator);
    defer stack.deinit();

    for (instructions) |instr| {
        if (instr.kind == @intFromEnum(ValueKind.reference)) {
            try stack.append(try clone_instruction(allocator, instr));
            continue;
        }

        var target = root;
        if (stack.items.len > 0) {
            const last = stack.items[stack.items.len - 1];
            if (last.kind == @intFromEnum(ValueKind.reference)) {
                const ref_id = switch (last.data) {
                    .reference => |id| id,
                    .string => |id| id,
                    else => "",
                };
                if (ref_id.len > 0) {
                    if (registry.get(ref_id)) |value| {
                        target = value;
                        _ = stack.pop();
                    }
                }
            }
        }

        if (target == null) return error.MissingTarget;

        const result = try apply_instruction(allocator, registry, target.?, instr);
        try stack.append(result);
    }

    if (stack.items.len == 0) return error.NoResult;
    const result = stack.items[0];
    if (stack.items.len > 1) {
        for (stack.items[1..]) |*item| deinit_instruction(allocator, item.*);
    }
    return result;
}

fn apply_instruction(
    allocator: std.mem.Allocator,
    registry: *registry_mod.ObjectRegistry,
    target: ProxyTarget,
    instr: ProxyInstruction,
) !ProxyInstruction {
    const kind = @as(InstructionKind, @enumFromInt(instr.kind));
    switch (kind) {
        .get => {
            const key = try helpers.parse_get_key(allocator, instr.data);
            defer allocator.free(key);
            const value = try target.get(allocator, key);
            return instructions_mod.create_value_instruction(allocator, value);
        },
        .apply => {
            const args = try helpers.parse_args(allocator, instr.data);
            defer {
                for (args) |*arg| arg.deinit(allocator);
                allocator.free(args);
            }
            const value = try target.apply(allocator, args);
            return instructions_mod.create_value_instruction(allocator, value);
        },
        .construct => {
            const args = try helpers.parse_args(allocator, instr.data);
            defer {
                for (args) |*arg| arg.deinit(allocator);
                allocator.free(args);
            }
            const value = try target.construct(allocator, args);
            return instructions_mod.create_value_instruction(allocator, value);
        },
        .release => {
            if (helpers.parse_release_id(allocator, instr.data)) |ref_id| {
                registry.release(ref_id);
                allocator.free(ref_id);
            }
            return instructions_mod.create_value_instruction(allocator, Value.undefined);
        },
        else => return error.UnsupportedInstruction,
    }
}

fn clone_instruction(allocator: std.mem.Allocator, instr: ProxyInstruction) !ProxyInstruction {
    const id = if (instr.id) |v| try allocator.dupe(u8, v) else null;
    const meta = if (instr.metadata) |v| try v.clone(allocator) else null;
    return ProxyInstruction{
        .id = id,
        .kind = instr.kind,
        .data = try instr.data.clone(allocator),
        .metadata = meta,
    };
}

fn deinit_instruction(allocator: std.mem.Allocator, instr: ProxyInstruction) void {
    if (instr.id) |id| allocator.free(id);
    instr.data.deinit(allocator);
    if (instr.metadata) |*meta| meta.deinit(allocator);
}
