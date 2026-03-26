const std = @import("std");
const types = @import("types.zig");
const instructions_mod = @import("instructions.zig");
const registry_mod = @import("registry.zig");
const stream_pool = @import("stream_pool.zig");
const transport = @import("transport.zig");
const codec_mod = @import("codec.zig");
const helpers = @import("helpers.zig");
const executor_mod = @import("executor.zig");
const cursor_mod = @import("cursor.zig");
const remote = @import("remote.zig");

pub const ProxyInstruction = types.ProxyInstruction;
pub const ProxyError = types.ProxyError;
pub const Value = types.Value;

pub const ImportedProxyable = struct {
    allocator: std.mem.Allocator,
    id: []const u8,
    session: transport.Session,
    registry: registry_mod.ObjectRegistry,
    stream_pool: stream_pool.StreamPool,
    codec: codec_mod.Codec,
    running: bool = false,
    thread: ?std.Thread = null,

    pub fn start(self: *ImportedProxyable) !void {
        if (self.running) return;
        self.running = true;
        self.thread = try std.Thread.spawn(.{}, accept_loop, .{self});
    }

    pub fn stop(self: *ImportedProxyable) void {
        self.running = false;
        self.session.close();
        self.stream_pool.close();
    }

    pub fn deinit(self: *ImportedProxyable) void {
        self.stop();
        self.registry.deinit();
        self.stream_pool.deinit();
        self.allocator.free(self.id);
    }

    pub fn root(self: *ImportedProxyable) cursor_mod.ProxyCursor {
        return cursor_mod.ProxyCursor.init(self.allocator, self.executor(), self.allocator.alloc(ProxyInstruction, 0) catch unreachable);
    }

    pub fn executor(self: *ImportedProxyable) cursor_mod.Executor {
        return .{ .ctx = self, .vtable = &executor_vtable };
    }

    pub fn export_target(self: *ImportedProxyable, target: registry_mod.ProxyTarget) !Value {
        const id = try self.registry.register(target);
        return Value{ .reference = try self.allocator.dupe(u8, id) };
    }

    fn accept_loop(self: *ImportedProxyable) void {
        while (self.running) {
            const stream = self.session.accept() catch break;
            _ = std.Thread.spawn(.{}, handle_stream, .{ self, stream }) catch {
                stream.close();
            };
        }
    }

    fn handle_stream(self: *ImportedProxyable, stream: transport.Stream) void {
        defer stream.close();

        while (self.running) {
            const instr = self.codec.read(self.allocator, stream) catch break;
            defer deinit_instruction(self.allocator, instr);
            const response = self.handle_instruction(instr) catch {
                const err_instr = instructions_mod.create_throw_instruction(self.allocator, ProxyError{ .message = "execution error" }) catch break;
                _ = self.codec.write(self.allocator, stream, err_instr) catch {};
                deinit_instruction(self.allocator, err_instr);
                break;
            };
            _ = self.codec.write(self.allocator, stream, response) catch break;
            deinit_instruction(self.allocator, response);
        }
    }

    fn handle_instruction(self: *ImportedProxyable, instr: ProxyInstruction) !ProxyInstruction {
        const kind = @as(types.InstructionKind, @enumFromInt(instr.kind));
        switch (kind) {
            .execute => {
                const instructions = try helpers.parse_instruction_list(self.allocator, instr.data);
                defer {
                    for (instructions) |*item| {
                        if (item.id) |id| self.allocator.free(id);
                        item.data.deinit(self.allocator);
                        if (item.metadata) |*meta| meta.deinit(self.allocator);
                    }
                    self.allocator.free(instructions);
                }
                const result = try executor_mod.evaluate_instructions(self.allocator, instructions, &self.registry, null);
                defer {
                    if (result.id) |id| self.allocator.free(id);
                    if (result.metadata) |*meta| meta.deinit(self.allocator);
                }
                return instructions_mod.create_return_instruction(self.allocator, result.data);
            },
            .release => {
                if (helpers.parse_release_id(self.allocator, instr.data)) |ref_id| {
                    self.registry.release(ref_id);
                    self.allocator.free(ref_id);
                }
                return instructions_mod.create_return_instruction(self.allocator, Value.undefined);
            },
            else => {
                return instructions_mod.create_throw_instruction(self.allocator, ProxyError{ .message = "unsupported instruction" });
            },
        }
    }
};

pub fn create_imported_proxyable(params: struct {
    allocator: std.mem.Allocator,
    session: transport.Session,
    codec: ?codec_mod.Codec = null,
    stream_pool_size: usize = 8,
    stream_pool_reuse: bool = true,
}) !*ImportedProxyable {
    const codec = params.codec orelse default_codec(params.allocator);
    const id = try @import("muid.zig").make(params.allocator);
    var imported = try params.allocator.create(ImportedProxyable);
    imported.* = .{
        .allocator = params.allocator,
        .id = id,
        .session = params.session,
        .registry = registry_mod.ObjectRegistry.init(params.allocator),
        .stream_pool = stream_pool.StreamPool.init(params.allocator, params.session, params.stream_pool_size, params.stream_pool_reuse),
        .codec = codec,
        .running = false,
        .thread = null,
    };
    try imported.start();
    return imported;
}

fn default_codec(allocator: std.mem.Allocator) codec_mod.Codec {
    _ = allocator;
    return default_json_codec.codec();
}

fn execute(ctx: *anyopaque, allocator: std.mem.Allocator, instructions: []const ProxyInstruction) anyerror!cursor_mod.ExecutionResult {
    const self: *ImportedProxyable = @ptrCast(@alignCast(ctx));
    const response = try remote.execute_remote(allocator, &self.stream_pool, self.codec, instructions);
    defer deinit_instruction(allocator, response);
    return remote.unwrap_response(allocator, self.executor(), response);
}

fn deinit_instruction(allocator: std.mem.Allocator, instr: ProxyInstruction) void {
    if (instr.id) |id| allocator.free(id);
    instr.data.deinit(allocator);
    if (instr.metadata) |*meta| meta.deinit(allocator);
}

const executor_vtable = cursor_mod.Executor.VTable{ .execute = execute };

var default_json_codec: codec_mod.JsonCodec = .{};
