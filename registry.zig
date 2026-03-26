const std = @import("std");
const types = @import("types.zig");
const muid = @import("muid.zig");

pub const Value = types.Value;

pub const ProxyTarget = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        get: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, name: []const u8) anyerror!Value,
        apply: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) anyerror!Value,
        construct: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) anyerror!Value,
    };

    pub fn get(self: ProxyTarget, allocator: std.mem.Allocator, name: []const u8) !Value {
        return self.vtable.get(self.ctx, allocator, name);
    }

    pub fn apply(self: ProxyTarget, allocator: std.mem.Allocator, args: []const Value) !Value {
        return self.vtable.apply(self.ctx, allocator, args);
    }

    pub fn construct(self: ProxyTarget, allocator: std.mem.Allocator, args: []const Value) !Value {
        return self.vtable.construct(self.ctx, allocator, args);
    }
};

const TargetKey = struct {
    ctx: *anyopaque,
    vtable: *const ProxyTarget.VTable,
};

pub const ObjectRegistry = struct {
    allocator: std.mem.Allocator,
    entries: std.StringHashMapUnmanaged(Entry) = .{},
    reverse: std.AutoHashMapUnmanaged(TargetKey, []const u8) = .{},

    const Entry = struct {
        target: ProxyTarget,
        count: usize,
    };

    pub fn init(allocator: std.mem.Allocator) ObjectRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ObjectRegistry) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.entries.deinit(self.allocator);
        self.reverse.deinit(self.allocator);
    }

    pub fn register(self: *ObjectRegistry, target: ProxyTarget) ![]const u8 {
        const key = TargetKey{ .ctx = target.ctx, .vtable = target.vtable };
        if (self.reverse.get(key)) |existing_id| {
            if (self.entries.getPtr(existing_id)) |entry| {
                entry.count += 1;
            }
            return existing_id;
        }

        const id = try muid.make(self.allocator);
        try self.entries.put(self.allocator, id, Entry{ .target = target, .count = 1 });
        try self.reverse.put(self.allocator, key, id);
        return id;
    }

    pub fn retain(self: *ObjectRegistry, id: []const u8) bool {
        if (self.entries.get(id)) |*entry| {
            entry.count += 1;
            return true;
        }
        return false;
    }

    pub fn get(self: *ObjectRegistry, id: []const u8) ?ProxyTarget {
        if (self.entries.get(id)) |entry| return entry.target;
        return null;
    }

    pub fn release(self: *ObjectRegistry, id: []const u8) void {
        if (self.entries.get(id)) |entry| {
            if (entry.count <= 1) {
                const key = TargetKey{ .ctx = entry.target.ctx, .vtable = entry.target.vtable };
                _ = self.reverse.remove(key);
                _ = self.entries.remove(id);
                self.allocator.free(id);
                return;
            }
            if (self.entries.getPtr(id)) |mutable| {
                mutable.count -= 1;
            }
        }
    }

    pub fn delete(self: *ObjectRegistry, id: []const u8) void {
        if (self.entries.get(id)) |entry| {
            const key = TargetKey{ .ctx = entry.target.ctx, .vtable = entry.target.vtable };
            _ = self.reverse.remove(key);
            _ = self.entries.remove(id);
            self.allocator.free(id);
        }
    }
};
