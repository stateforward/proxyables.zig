const std = @import("std");

const exported = @import("exported.zig");
const imported = @import("imported.zig");
const cursor = @import("cursor.zig");
const registry = @import("registry.zig");
const types = @import("types.zig");

pub const ExportedProxyable = exported.ExportedProxyable;
pub const ImportedProxyable = imported.ImportedProxyable;
pub const ProxyCursor = cursor.ProxyCursor;
pub const ProxyTarget = registry.ProxyTarget;
pub const Value = types.Value;
pub const ValueKind = types.ValueKind;
pub const InstructionKind = types.InstructionKind;
pub const ProxyInstruction = types.ProxyInstruction;
pub const ProxyError = types.ProxyError;

pub const Proxyable = struct {
    pub var exports: Registry = .{};
    pub var imports: Registry = .{};

    pub fn @"export"(params: anytype) !@TypeOf(exported.create_exported_proxyable(params)) {
        const proxy = try exported.create_exported_proxyable(params);
        try register(&exports, proxy);
        return proxy;
    }

    pub fn import_from(params: anytype) !cursor.ProxyCursor {
        const imported_proxy = try imported.create_imported_proxyable(params);
        try register(&imports, imported_proxy);
        return imported_proxy.root();
    }
};

const Registry = std.StringHashMapUnmanaged(*const anyopaque);

fn register(registry: *Registry, proxy: anytype) !void {
    const id = proxy_id(proxy);
    try registry.put(std.heap.page_allocator, id, to_anyopaque(proxy));
}

fn proxy_id(proxy: anytype) []const u8 {
    const T = @TypeOf(proxy);
    if (@typeInfo(T) != .Pointer) {
        @compileError("Proxyable registry expects proxy pointers with an `id` field.");
    }
    const P = @TypeOf(proxy.*);
    if (!@hasField(P, "id")) {
        @compileError("Proxy type must include an `id` field for registry lookup.");
    }
    const IdType = @TypeOf(@field(proxy.*, "id"));
    if (IdType != []const u8 and IdType != []u8) {
        @compileError("Proxy `id` field must be `[]const u8` or `[]u8`.");
    }
    return @field(proxy.*, "id");
}

fn to_anyopaque(proxy: anytype) *const anyopaque {
    const T = @TypeOf(proxy);
    if (@typeInfo(T) != .Pointer) {
        @compileError("Proxyable registry expects proxy pointers.");
    }
    return @ptrCast(*const anyopaque, proxy);
}
