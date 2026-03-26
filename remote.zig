const std = @import("std");
const types = @import("types.zig");
const instructions_mod = @import("instructions.zig");
const helpers = @import("helpers.zig");
const stream_pool = @import("stream_pool.zig");
const codec_mod = @import("codec.zig");
const cursor_mod = @import("cursor.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const InstructionKind = types.InstructionKind;
pub const ValueKind = types.ValueKind;
pub const Value = types.Value;
pub const ProxyError = types.ProxyError;

pub fn execute_remote(
    allocator: std.mem.Allocator,
    pool: *stream_pool.StreamPool,
    codec: codec_mod.Codec,
    instructions: []const ProxyInstruction,
) !ProxyInstruction {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var serialized = try arena.allocator().alloc(Value, instructions.len);
    for (instructions, 0..) |instr, i| {
        serialized[i] = try instructions_mod.instruction_to_value(arena.allocator(), instr);
    }

    const exec_instr = try instructions_mod.create_instruction_unsafe(arena.allocator(), .execute, Value{ .array = serialized });

    const stream = try pool.acquire();
    defer pool.release(stream);

    try codec.write(arena.allocator(), stream, exec_instr);
    return codec.read(allocator, stream);
}

pub fn unwrap_response(
    allocator: std.mem.Allocator,
    executor: cursor_mod.Executor,
    response: ProxyInstruction,
) cursor_mod.ExecutionResult {
    const kind = @as(InstructionKind, @enumFromInt(response.kind));
    switch (kind) {
        .throw => {
            const err = helpers.parse_proxy_error(allocator, response.data);
            return .{ .err = err };
        },
        .@"return" => {
            const value_instr = helpers.parse_instruction_value(allocator, response.data) catch {
                return .{ .err = ProxyError{ .message = "invalid return value" } };
            };
            if (value_instr.kind == @intFromEnum(ValueKind.reference)) {
                const ref_id = switch (value_instr.data) {
                    .reference => |id| id,
                    .string => |id| id,
                    else => "",
                };
                if (ref_id.len == 0) {
                    return .{ .err = ProxyError{ .message = "invalid reference id" } };
                }
                var list = allocator.alloc(ProxyInstruction, 1) catch {
                    return .{ .err = ProxyError{ .message = "allocation failed" } };
                };
                list[0] = value_instr;
                const cursor = cursor_mod.ProxyCursor.init(allocator, executor, list);
                return .{ .cursor = cursor };
            }
            const result_value = value_instr.data;
            if (value_instr.id) |id| allocator.free(id);
            if (value_instr.metadata) |*meta| meta.deinit(allocator);
            return .{ .value = result_value };
        },
        else => {
            return .{ .err = ProxyError{ .message = "unexpected response kind" } };
        },
    }
}
