const std = @import("std");
const types = @import("types.zig");
const instructions_mod = @import("instructions.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const ProxyError = types.ProxyError;
pub const InstructionKind = types.InstructionKind;
pub const ValueKind = types.ValueKind;
pub const Value = types.Value;

pub const ExecutionResult = struct {
    value: ?Value = null,
    cursor: ?ProxyCursor = null,
    err: ?ProxyError = null,
};

pub const Executor = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, instructions: []const ProxyInstruction) anyerror!ExecutionResult,
    };

    pub fn execute(self: Executor, allocator: std.mem.Allocator, instructions: []const ProxyInstruction) !ExecutionResult {
        return self.vtable.execute(self.ctx, allocator, instructions);
    }
};

pub const ProxyCursor = struct {
    allocator: std.mem.Allocator,
    executor: Executor,
    instructions: []ProxyInstruction,

    pub fn init(allocator: std.mem.Allocator, executor: Executor, instructions: []ProxyInstruction) ProxyCursor {
        return .{ .allocator = allocator, .executor = executor, .instructions = instructions };
    }

    pub fn deinit(self: *ProxyCursor) void {
        for (self.instructions) |*instr| {
            if (instr.id) |id| self.allocator.free(id);
            instr.data.deinit(self.allocator);
            if (instr.metadata) |*meta| meta.deinit(self.allocator);
        }
        self.allocator.free(self.instructions);
    }

    pub fn get(self: ProxyCursor, key: []const u8) !ProxyCursor {
        const key_val = Value{ .string = try self.allocator.dupe(u8, key) };
        var args = try self.allocator.alloc(Value, 1);
        args[0] = key_val;
        const instr = try instructions_mod.create_instruction_unsafe(self.allocator, .get, Value{ .array = args });
        const merged = try append_instruction(self.allocator, self.instructions, instr);
        return ProxyCursor.init(self.allocator, self.executor, merged);
    }

    pub fn apply(self: ProxyCursor, args: []const Value) !ProxyCursor {
        const instr = try build_args_instruction(self.allocator, .apply, args);
        const merged = try append_instruction(self.allocator, self.instructions, instr);
        return ProxyCursor.init(self.allocator, self.executor, merged);
    }

    pub fn construct(self: ProxyCursor, args: []const Value) !ProxyCursor {
        const instr = try build_args_instruction(self.allocator, .construct, args);
        const merged = try append_instruction(self.allocator, self.instructions, instr);
        return ProxyCursor.init(self.allocator, self.executor, merged);
    }

    pub fn exec(self: ProxyCursor) !ExecutionResult {
        return self.executor.execute(self.allocator, self.instructions);
    }

    pub fn reference_id(self: ProxyCursor) ?[]const u8 {
        if (self.instructions.len != 1) return null;
        const instr = self.instructions[0];
        if (instr.kind != @intFromEnum(ValueKind.reference)) return null;
        return switch (instr.data) {
            .reference => |id| id,
            .string => |id| id,
            else => null,
        };
    }
};

fn build_args_instruction(allocator: std.mem.Allocator, kind: InstructionKind, args: []const Value) !ProxyInstruction {
    var out = try allocator.alloc(Value, args.len);
    for (args, 0..) |arg, i| out[i] = try arg.clone(allocator);
    return instructions_mod.create_instruction_unsafe(allocator, kind, Value{ .array = out });
}

fn append_instruction(allocator: std.mem.Allocator, existing: []const ProxyInstruction, instr: ProxyInstruction) ![]ProxyInstruction {
    var out = try allocator.alloc(ProxyInstruction, existing.len + 1);
    for (existing, 0..) |item, i| out[i] = try clone_instruction(allocator, item);
    out[existing.len] = instr;
    return out;
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
