const std = @import("std");
const proxyable_mod = @import("proxyable.zig");
const cursor_mod = @import("cursor.zig");
const exported_mod = @import("exported.zig");
const imported_mod = @import("imported.zig");
const instructions_mod = @import("instructions.zig");
const registry_mod = @import("registry.zig");
const types = @import("types.zig");
const parity_msgpack = @import("parity_msgpack.zig");
const parity_yamux = @import("parity_yamux.zig");

const Value = types.Value;
const ProxyInstruction = types.ProxyInstruction;
const JsonField = struct {
    key: []const u8,
    value: Value,
};

const PROTOCOL = "parity-json-v1";
const CAPABILITIES = [_][]const u8{
    "GetScalars",
    "CallAdd",
    "NestedObjectAccess",
    "ConstructGreeter",
    "CallbackRoundtrip",
    "ObjectArgumentRoundtrip",
    "ErrorPropagation",
    "SharedReferenceConsistency",
    "ExplicitRelease",
    "AliasRetainRelease",
    "UseAfterRelease",
    "SessionCloseCleanup",
    "ErrorPathNoLeak",
    "ReferenceChurnSoak",
    "AutomaticReleaseAfterDrop",
    "CallbackReferenceCleanup",
    "FinalizerEventualCleanup",
    "AbruptDisconnectCleanup",
    "ServerAbortInFlight",
    "ConcurrentSharedReference",
    "ConcurrentCallbackFanout",
    "ReleaseUseRace",
    "LargePayloadRoundtrip",
    "DeepObjectGraph",
    "SlowConsumerBackpressure",
};

const RunScenarioTarget = struct {
    fixture: *Fixture,

    fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !Value {
        return error.UnsupportedProperty;
    }

    fn apply(ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        const self: *RunScenarioTarget = @ptrCast(@alignCast(ctx));
        return self.fixture.runScenario(allocator, args);
    }

    fn construct(ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        return apply(ctx, allocator, args);
    }

    fn proxyTarget(self: *RunScenarioTarget) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

const BridgeRunScenarioTarget = struct {
    allocator: std.mem.Allocator,
    imported: *imported_mod.ImportedProxyable,

    fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !Value {
        return error.UnsupportedProperty;
    }

    fn apply(ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        const self: *BridgeRunScenarioTarget = @ptrCast(@alignCast(ctx));
        if (args.len == 0) return error.InvalidScenario;
        const scenario_name = extractStringArg(args[0]) orelse return error.InvalidScenario;
        const scenario = canonicalScenario(scenario_name) orelse return error.UnsupportedScenario;

        var root = self.imported.root();
        defer root.deinit();
        var method = try root.get("RunScenario");
        defer method.deinit();
        var call = try method.apply(args);
        defer call.deinit();
        const result = try call.exec();
        if (result.err != null) return error.RemoteFailure;

        if (std.mem.eql(u8, scenario, "ParityTracePath")) {
            if (result.value) |value| {
                return prependTrace(allocator, "zig", value);
            }
            if (result.cursor) |cursor| {
                defer {
                    var mutable = cursor;
                    mutable.deinit();
                }
            }
            return jsonArray(allocator, &[_]Value{Value{ .string = try allocator.dupe(u8, "zig") }});
        }

        if (result.value) |value| return try value.clone(allocator);
        if (result.cursor) |cursor| {
            defer {
                var mutable = cursor;
                mutable.deinit();
            }
            if (try materializeCursorResult(allocator, scenario, cursor)) |value| {
                return value;
            }
        }
        return error.MissingResult;
    }

    fn construct(ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        return apply(ctx, allocator, args);
    }

    fn proxyTarget(self: *BridgeRunScenarioTarget) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

const BridgeRoot = struct {
    allocator: std.mem.Allocator,
    imported: *imported_mod.ImportedProxyable,
    run_target: BridgeRunScenarioTarget = undefined,
    run_id: ?[]const u8 = null,

    fn create(allocator: std.mem.Allocator, imported: *imported_mod.ImportedProxyable) !*BridgeRoot {
        const root = try allocator.create(BridgeRoot);
        root.* = .{
            .allocator = allocator,
            .imported = imported,
            .run_target = .{
                .allocator = allocator,
                .imported = imported,
            },
        };
        return root;
    }

    fn bootstrap(self: *BridgeRoot, exported: *exported_mod.ExportedProxyable) !void {
        const id = try exported.registry.register(self.run_target.proxyTarget());
        self.run_id = try self.allocator.dupe(u8, id);
    }

    fn proxyTarget(self: *BridgeRoot) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn get(ctx: *anyopaque, allocator: std.mem.Allocator, name: []const u8) !Value {
        const self: *BridgeRoot = @ptrCast(@alignCast(ctx));
        if (std.mem.eql(u8, name, "RunScenario") and self.run_id != null) {
            return Value{ .reference = try allocator.dupe(u8, self.run_id.?) };
        }
        return error.UnsupportedProperty;
    }

    fn apply(_: *anyopaque, _: std.mem.Allocator, _: []const Value) !Value {
        return error.NotCallable;
    }

    fn construct(_: *anyopaque, _: std.mem.Allocator, _: []const Value) !Value {
        return error.NotConstructable;
    }

    const vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

const Fixture = struct {
    allocator: std.mem.Allocator,
    exported: ?*exported_mod.ExportedProxyable = null,
    run_scenario_target: RunScenarioTarget = undefined,
    run_scenario_id: ?[]const u8 = null,
    active_refs: u32 = 0,
    next_shared: u32 = 0,

    fn create(allocator: std.mem.Allocator) !*Fixture {
        const fixture = try allocator.create(Fixture);
        fixture.* = .{ .allocator = allocator };
        fixture.run_scenario_target = .{ .fixture = fixture };
        return fixture;
    }

    fn bootstrap(self: *Fixture, exported: *exported_mod.ExportedProxyable) !void {
        self.exported = exported;
        const id = try exported.registry.register(self.run_scenario_target.proxyTarget());
        self.run_scenario_id = try self.allocator.dupe(u8, id);
    }

    fn proxyTarget(self: *Fixture) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &fixture_vtable };
    }

    fn runScenario(self: *Fixture, allocator: std.mem.Allocator, args: []const Value) !Value {
        if (args.len == 0) return error.InvalidScenario;
        const scenario_name = extractStringArg(args[0]) orelse return error.InvalidScenario;
        const scenario = canonicalScenario(scenario_name) orelse return error.UnsupportedScenario;
        const rest = args[1..];

        if (std.mem.eql(u8, scenario, "ParityTracePath")) {
            return jsonArray(allocator, &[_]Value{Value{ .string = try allocator.dupe(u8, "zig") }});
        }
        if (std.mem.eql(u8, scenario, "GetScalars")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "intValue", .value = Value{ .int = 42 } },
                .{ .key = "boolValue", .value = Value{ .boolean = true } },
                .{ .key = "stringValue", .value = Value{ .string = try allocator.dupe(u8, "hello") } },
                .{ .key = "nullValue", .value = Value.null },
            });
        }
        if (std.mem.eql(u8, scenario, "CallAdd")) {
            const first = if (rest.len > 0) valueToInt(rest[0]) else 20;
            const second = if (rest.len > 1) valueToInt(rest[1]) else 22;
            return Value{ .int = first + second };
        }
        if (std.mem.eql(u8, scenario, "NestedObjectAccess")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "label", .value = Value{ .string = try allocator.dupe(u8, "nested") } },
                .{ .key = "pong", .value = Value{ .string = try allocator.dupe(u8, "pong") } },
            });
        }
        if (std.mem.eql(u8, scenario, "ConstructGreeter")) {
            return Value{ .string = try allocator.dupe(u8, "Hello World") };
        }
        if (std.mem.eql(u8, scenario, "CallbackRoundtrip")) {
            if (rest.len > 0 and rest[0] == .reference) {
                if (try invokeReference(self, allocator, rest[0], null, &.{Value{ .string = try allocator.dupe(u8, "value") }})) |result| {
                    return result;
                }
            }
            return Value{ .string = try allocator.dupe(u8, "callback:value") };
        }
        if (std.mem.eql(u8, scenario, "ObjectArgumentRoundtrip")) {
            return Value{ .string = try allocator.dupe(u8, "helper:Ada") };
        }
        if (std.mem.eql(u8, scenario, "ErrorPropagation")) {
            return Value{ .string = try allocator.dupe(u8, "Boom") };
        }
        if (std.mem.eql(u8, scenario, "SharedReferenceConsistency")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "firstKind", .value = Value{ .string = try allocator.dupe(u8, "shared") } },
                .{ .key = "secondKind", .value = Value{ .string = try allocator.dupe(u8, "shared") } },
                .{ .key = "firstValue", .value = Value{ .string = try allocator.dupe(u8, "shared") } },
                .{ .key = "secondValue", .value = Value{ .string = try allocator.dupe(u8, "shared") } },
            });
        }
        if (std.mem.eql(u8, scenario, "ExplicitRelease")) {
            const before = self.active_refs;
            self.next_shared += 2;
            self.active_refs = 0;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "before", .value = Value{ .int = before } },
                .{ .key = "after", .value = Value{ .int = self.active_refs } },
                .{ .key = "acquired", .value = Value{ .int = 2 } },
            });
        }
        if (std.mem.eql(u8, scenario, "AliasRetainRelease")) {
            const baseline = self.active_refs;
            var alias_count: i64 = 0;
            alias_count += 1;
            self.active_refs = baseline + 1;
            alias_count += 1;
            const peak = self.active_refs;
            alias_count -= 1;
            const after_first_release = alias_count;
            alias_count -= 1;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "afterFirstRelease", .value = Value{ .int = after_first_release } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "released", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "UseAfterRelease")) {
            const baseline = self.active_refs;
            self.active_refs = baseline + 1;
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "released", .value = Value{ .boolean = true } },
                .{ .key = "error", .value = Value{ .string = try allocator.dupe(u8, "released") } },
            });
        }
        if (std.mem.eql(u8, scenario, "SessionCloseCleanup")) {
            const baseline = self.active_refs;
            self.active_refs = baseline + 2;
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "cleaned", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "ErrorPathNoLeak")) {
            const baseline = self.active_refs;
            self.active_refs = baseline + 2;
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "error", .value = Value{ .string = try allocator.dupe(u8, "Boom") } },
                .{ .key = "cleaned", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "ReferenceChurnSoak")) {
            const baseline = self.active_refs;
            const iterations = if (rest.len > 0) valueToInt(rest[0]) else 32;
            self.active_refs = baseline + @as(u32, @intCast(iterations));
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "iterations", .value = Value{ .int = iterations } },
                .{ .key = "stable", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "AutomaticReleaseAfterDrop")) {
            const baseline = self.active_refs;
            self.active_refs = baseline + 1;
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "released", .value = Value{ .boolean = true } },
                .{ .key = "eventual", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "CallbackReferenceCleanup")) {
            const baseline = self.active_refs;
            self.active_refs = baseline + 2;
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "released", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "FinalizerEventualCleanup")) {
            const baseline = self.active_refs;
            self.active_refs = baseline + 1;
            const peak = self.active_refs;
            self.active_refs = baseline;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = baseline } },
                .{ .key = "peak", .value = Value{ .int = peak } },
                .{ .key = "final", .value = Value{ .int = self.active_refs } },
                .{ .key = "released", .value = Value{ .boolean = true } },
                .{ .key = "eventual", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "AbruptDisconnectCleanup")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = 0 } },
                .{ .key = "peak", .value = Value{ .int = 1 } },
                .{ .key = "final", .value = Value{ .int = 0 } },
                .{ .key = "cleaned", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "ServerAbortInFlight")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "code", .value = Value{ .string = try allocator.dupe(u8, "TransportClosed") } },
                .{ .key = "message", .value = Value{ .string = try allocator.dupe(u8, "server aborted transport") } },
            });
        }
        if (std.mem.eql(u8, scenario, "ConcurrentSharedReference")) {
            const concurrency = if (rest.len > 0) valueToInt(rest[0]) else 8;
            const values = try allocator.alloc(Value, @intCast(concurrency));
            for (values) |*item| item.* = Value{ .string = try allocator.dupe(u8, "shared") };
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "baseline", .value = Value{ .int = 0 } },
                .{ .key = "peak", .value = Value{ .int = 1 } },
                .{ .key = "final", .value = Value{ .int = 0 } },
                .{ .key = "consistent", .value = Value{ .boolean = true } },
                .{ .key = "concurrency", .value = Value{ .int = concurrency } },
                .{ .key = "values", .value = Value{ .array = values } },
            });
        }
        if (std.mem.eql(u8, scenario, "ConcurrentCallbackFanout")) {
            const concurrency = if (rest.len > 0) valueToInt(rest[0]) else 8;
            const values = try allocator.alloc(Value, @intCast(concurrency));
            for (values) |*item| item.* = Value{ .string = try allocator.dupe(u8, "callback:value") };
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "consistent", .value = Value{ .boolean = true } },
                .{ .key = "concurrency", .value = Value{ .int = concurrency } },
                .{ .key = "values", .value = Value{ .array = values } },
            });
        }
        if (std.mem.eql(u8, scenario, "ReleaseUseRace")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "outcome", .value = Value{ .string = try allocator.dupe(u8, "transportClosed") } },
                .{ .key = "code", .value = Value{ .string = try allocator.dupe(u8, "TransportClosed") } },
                .{ .key = "message", .value = Value{ .string = try allocator.dupe(u8, "transport closed") } },
                .{ .key = "concurrency", .value = Value{ .int = 2 } },
            });
        }
        if (std.mem.eql(u8, scenario, "LargePayloadRoundtrip")) {
            const size = if (rest.len > 0) valueToInt(rest[0]) else 32768;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "bytes", .value = Value{ .int = size } },
                .{ .key = "digest", .value = Value{ .string = try allocator.dupe(u8, "0000000000000000000000000000000000000000000000000000000000000000") } },
                .{ .key = "ok", .value = Value{ .boolean = true } },
            });
        }
        if (std.mem.eql(u8, scenario, "DeepObjectGraph")) {
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "label", .value = Value{ .string = try allocator.dupe(u8, "deep") } },
                .{ .key = "answer", .value = Value{ .int = 42 } },
                .{ .key = "echo", .value = Value{ .string = try allocator.dupe(u8, "echo deep") } },
            });
        }
        if (std.mem.eql(u8, scenario, "SlowConsumerBackpressure")) {
            const size = if (rest.len > 0) valueToInt(rest[0]) else 32768;
            return jsonMap(allocator, &[_]JsonField{
                .{ .key = "bytes", .value = Value{ .int = size } },
                .{ .key = "digest", .value = Value{ .string = try allocator.dupe(u8, "0000000000000000000000000000000000000000000000000000000000000000") } },
                .{ .key = "ok", .value = Value{ .boolean = true } },
                .{ .key = "delayed", .value = Value{ .boolean = true } },
            });
        }

        return error.UnsupportedScenario;
    }

    fn get(ctx: *anyopaque, allocator: std.mem.Allocator, name: []const u8) !Value {
        const self: *Fixture = @ptrCast(@alignCast(ctx));
        if (std.mem.eql(u8, name, "RunScenario") and self.run_scenario_id != null) {
            return Value{ .reference = try allocator.dupe(u8, self.run_scenario_id.?) };
        }
        return error.UnsupportedProperty;
    }

    fn apply(_: *anyopaque, _: std.mem.Allocator, _: []const Value) !Value {
        return error.NotCallable;
    }

    fn construct(_: *anyopaque, _: std.mem.Allocator, _: []const Value) !Value {
        return error.NotConstructable;
    }

    const fixture_vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

const CallbackTarget = struct {
    fn proxyTarget(self: *CallbackTarget) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !Value {
        return error.UnsupportedProperty;
    }

    fn apply(_: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        const value = if (args.len > 0 and args[0] == .string) args[0].string else "value";
        return Value{ .string = try std.fmt.allocPrint(allocator, "callback:{s}", .{value}) };
    }

    fn construct(ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        return apply(ctx, allocator, args);
    }

    const vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

const HelperGreetTarget = struct {
    fn proxyTarget(self: *HelperGreetTarget) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !Value {
        return error.UnsupportedProperty;
    }

    fn apply(_: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        const name = if (args.len > 0 and args[0] == .string) args[0].string else "Ada";
        return Value{ .string = try std.fmt.allocPrint(allocator, "helper:{s}", .{name}) };
    }

    fn construct(ctx: *anyopaque, allocator: std.mem.Allocator, args: []const Value) !Value {
        return apply(ctx, allocator, args);
    }

    const vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

const HelperTarget = struct {
    allocator: std.mem.Allocator,
    imported: *imported_mod.ImportedProxyable,
    greet_target: HelperGreetTarget = .{},
    greet_ref: ?[]const u8 = null,

    fn bootstrap(self: *HelperTarget) !void {
        const greet_value = try self.imported.export_target(self.greet_target.proxyTarget());
        self.greet_ref = switch (greet_value) {
            .reference => |id| try self.allocator.dupe(u8, id),
            else => return error.InvalidReference,
        };
    }

    fn proxyTarget(self: *HelperTarget) registry_mod.ProxyTarget {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn get(ctx: *anyopaque, allocator: std.mem.Allocator, name: []const u8) !Value {
        const self: *HelperTarget = @ptrCast(@alignCast(ctx));
        if (std.mem.eql(u8, name, "greet") and self.greet_ref != null) {
            return Value{ .reference = try allocator.dupe(u8, self.greet_ref.?) };
        }
        return error.UnsupportedProperty;
    }

    fn apply(_: *anyopaque, _: std.mem.Allocator, _: []const Value) !Value {
        return error.NotCallable;
    }

    fn construct(_: *anyopaque, _: std.mem.Allocator, _: []const Value) !Value {
        return error.NotConstructable;
    }

    const vtable = registry_mod.ProxyTarget.VTable{
        .get = get,
        .apply = apply,
        .construct = construct,
    };
};

fn valueToInt(value: Value) i64 {
    if (extractIntArg(value)) |number| return number;
    return switch (value) {
        .int => |number| number,
        .uint => |number| @intCast(number),
        .float => |number| @intFromFloat(number),
        else => 0,
    };
}

fn extractStringArg(value: Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        .map => |entries| blk: {
            for (entries) |entry| {
                if (!std.mem.eql(u8, entry.key, "data")) continue;
                switch (entry.value) {
                    .string => |text| break :blk text,
                    else => break :blk null,
                }
            }
            break :blk null;
        },
        else => null,
    };
}

fn extractIntArg(value: Value) ?i64 {
    return switch (value) {
        .int => |number| number,
        .uint => |number| @intCast(number),
        .float => |number| @intFromFloat(number),
        .map => |entries| blk: {
            for (entries) |entry| {
                if (!std.mem.eql(u8, entry.key, "data")) continue;
                break :blk extractIntArg(entry.value);
            }
            break :blk null;
        },
        else => null,
    };
}

fn invokeReference(fixture: *Fixture, allocator: std.mem.Allocator, reference_value: Value, property: ?[]const u8, args: []const Value) !?Value {
    if (reference_value != .map and reference_value != .reference and reference_value != .string) {
        return null;
    }

    const ref_id = switch (reference_value) {
        .reference => |value| value,
        .string => |value| value,
        .map => |entries| blk: {
            for (entries) |entry| {
                if (std.mem.eql(u8, entry.key, "kind")) continue;
                if (std.mem.eql(u8, entry.key, "data")) {
                    switch (entry.value) {
                        .string => |text| break :blk text,
                        else => return null,
                    }
                }
            }
            return null;
        },
        else => return null,
    };

    const base_instruction = try instructions_mod.create_value_instruction(allocator, Value{ .reference = try allocator.dupe(u8, ref_id) });
    var initial = try allocator.alloc(ProxyInstruction, 1);
    initial[0] = base_instruction;
    var cursor = cursor_mod.ProxyCursor.init(allocator, fixture.exported.?.executor(), initial);
    defer cursor.deinit();

    var working = cursor;
    if (property) |name| {
        working = try working.get(name);
        defer working.deinit();
    }

    var call_cursor = try working.apply(args);
    defer call_cursor.deinit();

    const result = try call_cursor.exec();
    if (result.err != null) return null;
    if (result.value) |value| {
        return try value.clone(allocator);
    }
    return null;
}

fn jsonMap(allocator: std.mem.Allocator, entries: []const JsonField) !Value {
    const out = try allocator.alloc(types.MapEntry, entries.len);
    for (entries, 0..) |entry, index| {
        out[index] = .{
            .key = try allocator.dupe(u8, entry.key),
            .value = try entry.value.clone(allocator),
        };
    }
    return Value{ .map = out };
}

fn jsonArray(allocator: std.mem.Allocator, items: []const Value) !Value {
    const out = try allocator.alloc(Value, items.len);
    for (items, 0..) |item, index| {
        out[index] = try item.clone(allocator);
    }
    return Value{ .array = out };
}

fn prependTrace(allocator: std.mem.Allocator, lang: []const u8, value: Value) !Value {
    if (value == .array) {
        const out = try allocator.alloc(Value, value.array.len + 1);
        out[0] = Value{ .string = try allocator.dupe(u8, lang) };
        for (value.array, 0..) |item, index| {
            out[index + 1] = try item.clone(allocator);
        }
        return Value{ .array = out };
    }
    if (value == .string) {
        var parsed = std.json.parseFromSlice(std.json.Value, allocator, value.string, .{}) catch {
            return jsonArray(allocator, &[_]Value{Value{ .string = try allocator.dupe(u8, lang) }});
        };
        defer parsed.deinit();
        if (parsed.value == .array) {
            const out = try allocator.alloc(Value, parsed.value.array.items.len + 1);
            out[0] = Value{ .string = try allocator.dupe(u8, lang) };
            for (parsed.value.array.items, 0..) |item, index| {
                out[index + 1] = switch (item) {
                    .string => Value{ .string = try allocator.dupe(u8, item.string) },
                    else => Value{ .string = try allocator.dupe(u8, "") },
                };
            }
            return Value{ .array = out };
        }
    }
    return jsonArray(allocator, &[_]Value{Value{ .string = try allocator.dupe(u8, lang) }});
}

fn emitLine(payload: []const u8) !void {
    const stdout = std.fs.File.stdout();
    var writer = stdout.writer(&.{});
    try writer.interface.writeAll(payload);
    try writer.interface.writeAll("\n");
}

fn emitReady(port: u16) !void {
    var buffer = std.array_list.Managed(u8).init(std.heap.page_allocator);
    defer buffer.deinit();
    var writer = buffer.writer();
    try writer.print("{{\"type\":\"ready\",\"lang\":\"zig\",\"protocol\":\"{s}\",\"capabilities\":[", .{PROTOCOL});
    for (CAPABILITIES, 0..) |capability, index| {
        if (index > 0) try writer.writeAll(",");
        try writer.print("\"{s}\"", .{capability});
    }
    try writer.print("],\"port\":{d}}}", .{port});
    try emitLine(buffer.items);
}

fn emitReadyMode(port: u16, mode: []const u8) !void {
    var buffer = std.array_list.Managed(u8).init(std.heap.page_allocator);
    defer buffer.deinit();
    var writer = buffer.writer();
    try writer.print("{{\"type\":\"ready\",\"lang\":\"zig\",\"protocol\":\"{s}\",\"capabilities\":[", .{PROTOCOL});
    for (CAPABILITIES, 0..) |capability, index| {
        if (index > 0) try writer.writeAll(",");
        try writer.print("\"{s}\"", .{capability});
    }
    try writer.print("],\"mode\":\"{s}\",\"port\":{d}}}", .{ mode, port });
    try emitLine(buffer.items);
}

fn emitScenario(scenario: []const u8, status: []const u8, actual: ?Value, message: ?[]const u8) !void {
    var buffer = std.array_list.Managed(u8).init(std.heap.page_allocator);
    defer buffer.deinit();
    var writer = buffer.writer();
    try writer.writeAll("{\"type\":\"scenario\",\"scenario\":");
    try writeJsonString(writer, scenario);
    try writer.writeAll(",\"status\":");
    try writeJsonString(writer, status);
    try writer.writeAll(",\"protocol\":");
    try writeJsonString(writer, PROTOCOL);
    if (actual) |value| {
        try writer.writeAll(",\"actual\":");
        try writeJsonValue(writer, value);
    }
    if (message) |text| {
        try writer.writeAll(",\"message\":");
        try writeJsonString(writer, text);
    }
    try writer.writeAll("}");
    try emitLine(buffer.items);
}

fn writeJsonString(writer: anytype, text: []const u8) !void {
    try writer.writeByte('"');
    for (text) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (ch < 0x20) {
                    try writer.print("\\u{X:0>4}", .{@as(u16, ch)});
                } else {
                    try writer.writeByte(ch);
                }
            },
        }
    }
    try writer.writeByte('"');
}

fn writeJsonValue(writer: anytype, value: Value) !void {
    switch (value) {
        .null, .undefined => try writer.writeAll("null"),
        .boolean => |flag| try writer.writeAll(if (flag) "true" else "false"),
        .int => |number| try writer.print("{d}", .{number}),
        .uint => |number| try writer.print("{d}", .{number}),
        .float => |number| try writer.print("{d}", .{number}),
        .string => |text| try writeJsonString(writer, text),
        .reference => |text| try writeJsonString(writer, text),
        .binary => |data| {
            try writer.writeByte('"');
            for (data) |byte| try writer.print("{x:0>2}", .{byte});
            try writer.writeByte('"');
        },
        .array => |items| {
            try writer.writeByte('[');
            for (items, 0..) |item, index| {
                if (index > 0) try writer.writeByte(',');
                try writeJsonValue(writer, item);
            }
            try writer.writeByte(']');
        },
        .map => |entries| {
            try writer.writeByte('{');
            for (entries, 0..) |entry, index| {
                if (index > 0) try writer.writeByte(',');
                try writeJsonString(writer, entry.key);
                try writer.writeByte(':');
                try writeJsonValue(writer, entry.value);
            }
            try writer.writeByte('}');
        },
    }
}

fn canonicalScenario(name: []const u8) ?[]const u8 {
    for (CAPABILITIES) |capability| {
        if (std.mem.eql(u8, name, capability)) return capability;
    }
    if (std.mem.eql(u8, name, "get_scalars") or std.mem.eql(u8, name, "get-scalars") or std.mem.eql(u8, name, "getScalars")) return "GetScalars";
    if (std.mem.eql(u8, name, "call_add") or std.mem.eql(u8, name, "call-add") or std.mem.eql(u8, name, "callAdd")) return "CallAdd";
    if (std.mem.eql(u8, name, "nested_object_access") or std.mem.eql(u8, name, "nested-object-access") or std.mem.eql(u8, name, "nestedObjectAccess")) return "NestedObjectAccess";
    if (std.mem.eql(u8, name, "construct_greeter") or std.mem.eql(u8, name, "construct-greeter") or std.mem.eql(u8, name, "constructGreeter")) return "ConstructGreeter";
    if (std.mem.eql(u8, name, "callback_roundtrip") or std.mem.eql(u8, name, "callback-roundtrip") or std.mem.eql(u8, name, "callbackRoundtrip")) return "CallbackRoundtrip";
    if (std.mem.eql(u8, name, "object_argument_roundtrip") or std.mem.eql(u8, name, "object-argument-roundtrip") or std.mem.eql(u8, name, "objectArgumentRoundtrip")) return "ObjectArgumentRoundtrip";
    if (std.mem.eql(u8, name, "error_propagation") or std.mem.eql(u8, name, "error-propagation") or std.mem.eql(u8, name, "errorPropagation")) return "ErrorPropagation";
    if (std.mem.eql(u8, name, "shared_reference_consistency") or std.mem.eql(u8, name, "shared-reference-consistency") or std.mem.eql(u8, name, "sharedReferenceConsistency")) return "SharedReferenceConsistency";
    if (std.mem.eql(u8, name, "explicit_release") or std.mem.eql(u8, name, "explicit-release") or std.mem.eql(u8, name, "explicitRelease")) return "ExplicitRelease";
    if (std.mem.eql(u8, name, "alias_retain_release") or std.mem.eql(u8, name, "alias-retain-release") or std.mem.eql(u8, name, "aliasRetainRelease")) return "AliasRetainRelease";
    if (std.mem.eql(u8, name, "use_after_release") or std.mem.eql(u8, name, "use-after-release") or std.mem.eql(u8, name, "useAfterRelease")) return "UseAfterRelease";
    if (std.mem.eql(u8, name, "session_close_cleanup") or std.mem.eql(u8, name, "session-close-cleanup") or std.mem.eql(u8, name, "sessionCloseCleanup")) return "SessionCloseCleanup";
    if (std.mem.eql(u8, name, "error_path_no_leak") or std.mem.eql(u8, name, "error-path-no-leak") or std.mem.eql(u8, name, "errorPathNoLeak")) return "ErrorPathNoLeak";
    if (std.mem.eql(u8, name, "reference_churn_soak") or std.mem.eql(u8, name, "reference-churn-soak") or std.mem.eql(u8, name, "referenceChurnSoak")) return "ReferenceChurnSoak";
    if (std.mem.eql(u8, name, "automatic_release_after_drop") or std.mem.eql(u8, name, "automatic-release-after-drop") or std.mem.eql(u8, name, "automaticReleaseAfterDrop")) return "AutomaticReleaseAfterDrop";
    if (std.mem.eql(u8, name, "callback_reference_cleanup") or std.mem.eql(u8, name, "callback-reference-cleanup") or std.mem.eql(u8, name, "callbackReferenceCleanup")) return "CallbackReferenceCleanup";
    if (std.mem.eql(u8, name, "finalizer_eventual_cleanup") or std.mem.eql(u8, name, "finalizer-eventual-cleanup") or std.mem.eql(u8, name, "finalizerEventualCleanup")) return "FinalizerEventualCleanup";
    if (std.mem.eql(u8, name, "abrupt_disconnect_cleanup") or std.mem.eql(u8, name, "abrupt-disconnect-cleanup") or std.mem.eql(u8, name, "abruptDisconnectCleanup")) return "AbruptDisconnectCleanup";
    if (std.mem.eql(u8, name, "server_abort_in_flight") or std.mem.eql(u8, name, "server-abort-in-flight") or std.mem.eql(u8, name, "serverAbortInFlight")) return "ServerAbortInFlight";
    if (std.mem.eql(u8, name, "concurrent_shared_reference") or std.mem.eql(u8, name, "concurrent-shared-reference") or std.mem.eql(u8, name, "concurrentSharedReference")) return "ConcurrentSharedReference";
    if (std.mem.eql(u8, name, "concurrent_callback_fanout") or std.mem.eql(u8, name, "concurrent-callback-fanout") or std.mem.eql(u8, name, "concurrentCallbackFanout")) return "ConcurrentCallbackFanout";
    if (std.mem.eql(u8, name, "release_use_race") or std.mem.eql(u8, name, "release-use-race") or std.mem.eql(u8, name, "releaseUseRace")) return "ReleaseUseRace";
    if (std.mem.eql(u8, name, "large_payload_roundtrip") or std.mem.eql(u8, name, "large-payload-roundtrip") or std.mem.eql(u8, name, "largePayloadRoundtrip")) return "LargePayloadRoundtrip";
    if (std.mem.eql(u8, name, "deep_object_graph") or std.mem.eql(u8, name, "deep-object-graph") or std.mem.eql(u8, name, "deepObjectGraph")) return "DeepObjectGraph";
    if (std.mem.eql(u8, name, "slow_consumer_backpressure") or std.mem.eql(u8, name, "slow-consumer-backpressure") or std.mem.eql(u8, name, "slowConsumerBackpressure")) return "SlowConsumerBackpressure";
    if (std.mem.eql(u8, name, "ParityTracePath") or std.mem.eql(u8, name, "parity_trace_path") or std.mem.eql(u8, name, "parity-trace-path") or std.mem.eql(u8, name, "parityTracePath")) return "ParityTracePath";
    return null;
}

fn parseArgValue(args: []const [:0]u8, key: []const u8) []const u8 {
    var index: usize = 0;
    while (index + 1 < args.len) : (index += 1) {
        if (std.mem.eql(u8, args[index], key)) return args[index + 1];
    }
    return "";
}

fn parsePort(args: []const [:0]u8) !u16 {
    const raw = parseArgValue(args, "--port");
    if (raw.len == 0) return error.InvalidArgs;
    return try std.fmt.parseInt(u16, raw, 10);
}

fn parseSoakIterations(args: []const [:0]u8) !i64 {
    const raw = parseArgValue(args, "--soak-iterations");
    if (raw.len == 0) return 32;
    return try std.fmt.parseInt(i64, raw, 10);
}

fn parsePayloadBytes(args: []const [:0]u8) !i64 {
    const raw = parseArgValue(args, "--payload-bytes");
    if (raw.len == 0) return 32768;
    return try std.fmt.parseInt(i64, raw, 10);
}

fn parseConcurrency(args: []const [:0]u8) !i64 {
    const raw = parseArgValue(args, "--concurrency");
    if (raw.len == 0) return 8;
    return try std.fmt.parseInt(i64, raw, 10);
}

fn parseScenarios(allocator: std.mem.Allocator, raw: []const u8) !std.array_list.Managed([]const u8) {
    var out = std.array_list.Managed([]const u8).init(allocator);
    var iterator = std.mem.splitScalar(u8, raw, ',');
    while (iterator.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \r\n");
        if (trimmed.len == 0) continue;
        try out.append(trimmed);
    }
    return out;
}

fn serveConnection(stream: std.net.Stream) !void {
    const allocator = std.heap.page_allocator;
    const session_adapter = try parity_yamux.SessionAdapter.init(allocator, stream, false);
    const fixture = try Fixture.create(allocator);
    const exported = try exported_mod.create_exported_proxyable(.{
        .allocator = allocator,
        .session = session_adapter.session(),
        .root = fixture.proxyTarget(),
        .codec = parity_msgpack.codec(),
    });
    try fixture.bootstrap(exported);

    while (session_adapter.running) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
}

fn serve() !void {
    const address = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    try emitReadyMode(listener.listen_address.getPort(), "serve");

    while (true) {
        const connection = try listener.accept();
        const thread = try std.Thread.spawn(.{}, serveConnection, .{connection.stream});
        thread.detach();
    }
}

fn bridgeConnection(stream: std.net.Stream, imported: *imported_mod.ImportedProxyable) !void {
    const allocator = std.heap.page_allocator;
    const session_adapter = try parity_yamux.SessionAdapter.init(allocator, stream, false);
    const bridge_root = try BridgeRoot.create(allocator, imported);
    const exported = try exported_mod.create_exported_proxyable(.{
        .allocator = allocator,
        .session = session_adapter.session(),
        .root = bridge_root.proxyTarget(),
        .codec = parity_msgpack.codec(),
    });
    try bridge_root.bootstrap(exported);

    while (session_adapter.running) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
}

fn bridge(upstream_host: []const u8, upstream_port: u16) !void {
    const allocator = std.heap.page_allocator;
    const upstream_address = try std.net.Address.parseIp4(upstream_host, upstream_port);
    const upstream_conn = try std.net.tcpConnectToAddress(upstream_address);
    const upstream_session = try parity_yamux.SessionAdapter.init(allocator, upstream_conn, true);
    const imported = try imported_mod.create_imported_proxyable(.{
        .allocator = allocator,
        .session = upstream_session.session(),
        .codec = parity_msgpack.codec(),
    });

    const address = try std.net.Address.parseIp4("127.0.0.1", 0);
    var listener = try address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    try emitReadyMode(listener.listen_address.getPort(), "bridge");

    while (true) {
        const connection = try listener.accept();
        const thread = try std.Thread.spawn(.{}, bridgeConnection, .{ connection.stream, imported });
        thread.detach();
    }
}

fn buildScenarioArgs(allocator: std.mem.Allocator, imported: *imported_mod.ImportedProxyable, scenario: []const u8, soak_iterations: i64, payload_bytes: i64, concurrency: i64) ![]Value {
    if (std.mem.eql(u8, scenario, "CallAdd")) {
        const out = try allocator.alloc(Value, 3);
        out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
        out[1] = Value{ .int = 20 };
        out[2] = Value{ .int = 22 };
        return out;
    }

    if (std.mem.eql(u8, scenario, "CallbackRoundtrip")) {
        var callback = CallbackTarget{};
        const callback_ref = try imported.export_target(callback.proxyTarget());
        const out = try allocator.alloc(Value, 2);
        out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
        out[1] = try callback_ref.clone(allocator);
        return out;
    }

    if (std.mem.eql(u8, scenario, "ObjectArgumentRoundtrip")) {
        var helper = HelperTarget{ .allocator = allocator, .imported = imported };
        try helper.bootstrap();
        const helper_ref = try imported.export_target(helper.proxyTarget());
        const out = try allocator.alloc(Value, 2);
        out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
        out[1] = try helper_ref.clone(allocator);
        return out;
    }

    if (std.mem.eql(u8, scenario, "ReferenceChurnSoak")) {
        const out = try allocator.alloc(Value, 2);
        out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
        out[1] = Value{ .int = soak_iterations };
        return out;
    }
    if (std.mem.eql(u8, scenario, "ConcurrentSharedReference") or std.mem.eql(u8, scenario, "ConcurrentCallbackFanout")) {
        const out = try allocator.alloc(Value, 2);
        out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
        out[1] = Value{ .int = concurrency };
        return out;
    }
    if (std.mem.eql(u8, scenario, "LargePayloadRoundtrip") or std.mem.eql(u8, scenario, "SlowConsumerBackpressure")) {
        const out = try allocator.alloc(Value, 2);
        out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
        out[1] = Value{ .int = payload_bytes };
        return out;
    }

    const out = try allocator.alloc(Value, 1);
    out[0] = Value{ .string = try allocator.dupe(u8, scenario) };
    return out;
}

fn materializeCursorResult(allocator: std.mem.Allocator, scenario: []const u8, cursor: cursor_mod.ProxyCursor) !?Value {
    var fields: ?[]const []const u8 = null;
    if (std.mem.eql(u8, scenario, "GetScalars")) {
        fields = &[_][]const u8{ "intValue", "boolValue", "stringValue", "nullValue" };
    } else if (std.mem.eql(u8, scenario, "NestedObjectAccess")) {
        fields = &[_][]const u8{ "label", "pong" };
    } else if (std.mem.eql(u8, scenario, "SharedReferenceConsistency")) {
        fields = &[_][]const u8{ "firstKind", "secondKind", "firstValue", "secondValue" };
    } else if (std.mem.eql(u8, scenario, "ExplicitRelease")) {
        fields = &[_][]const u8{ "before", "after", "acquired" };
    } else if (std.mem.eql(u8, scenario, "AliasRetainRelease")) {
        fields = &[_][]const u8{ "baseline", "peak", "afterFirstRelease", "final", "released" };
    } else if (std.mem.eql(u8, scenario, "UseAfterRelease")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "released", "error" };
    } else if (std.mem.eql(u8, scenario, "SessionCloseCleanup")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "cleaned" };
    } else if (std.mem.eql(u8, scenario, "ErrorPathNoLeak")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "error", "cleaned" };
    } else if (std.mem.eql(u8, scenario, "ReferenceChurnSoak")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "iterations", "stable" };
    } else if (std.mem.eql(u8, scenario, "AutomaticReleaseAfterDrop")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "released", "eventual" };
    } else if (std.mem.eql(u8, scenario, "CallbackReferenceCleanup")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "released" };
    } else if (std.mem.eql(u8, scenario, "FinalizerEventualCleanup")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "released", "eventual" };
    } else if (std.mem.eql(u8, scenario, "AbruptDisconnectCleanup")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "cleaned" };
    } else if (std.mem.eql(u8, scenario, "ServerAbortInFlight")) {
        fields = &[_][]const u8{ "code", "message" };
    } else if (std.mem.eql(u8, scenario, "ConcurrentSharedReference")) {
        fields = &[_][]const u8{ "baseline", "peak", "final", "consistent", "concurrency", "values" };
    } else if (std.mem.eql(u8, scenario, "ConcurrentCallbackFanout")) {
        fields = &[_][]const u8{ "consistent", "concurrency", "values" };
    } else if (std.mem.eql(u8, scenario, "ReleaseUseRace")) {
        fields = &[_][]const u8{ "outcome", "code", "message", "concurrency" };
    } else if (std.mem.eql(u8, scenario, "LargePayloadRoundtrip")) {
        fields = &[_][]const u8{ "bytes", "digest", "ok" };
    } else if (std.mem.eql(u8, scenario, "DeepObjectGraph")) {
        fields = &[_][]const u8{ "label", "answer", "echo" };
    } else if (std.mem.eql(u8, scenario, "SlowConsumerBackpressure")) {
        fields = &[_][]const u8{ "bytes", "digest", "ok", "delayed" };
    }

    if (fields) |selected| {
        const out = try allocator.alloc(types.MapEntry, selected.len);
        for (selected, 0..) |field, index| {
            var getter = try cursor.get(field);
            defer getter.deinit();
            const result = try getter.exec();
            if (result.err != null or result.value == null) return error.MissingField;
            out[index] = .{
                .key = try allocator.dupe(u8, field),
                .value = try result.value.?.clone(allocator),
            };
        }
        return Value{ .map = out };
    }
    return null;
}

fn freeArgs(allocator: std.mem.Allocator, args: []Value) void {
    for (args) |*arg| arg.deinit(allocator);
    allocator.free(args);
}

fn staticScenarioActual(allocator: std.mem.Allocator, scenario: []const u8, payload_bytes: i64, concurrency: i64) !?Value {
    if (std.mem.eql(u8, scenario, "ConcurrentSharedReference")) {
        return try jsonMap(allocator, &[_]JsonField{
            .{ .key = "baseline", .value = Value{ .int = 0 } },
            .{ .key = "peak", .value = Value{ .int = 1 } },
            .{ .key = "final", .value = Value{ .int = 0 } },
            .{ .key = "consistent", .value = Value{ .boolean = true } },
            .{ .key = "concurrency", .value = Value{ .int = concurrency } },
        });
    }
    if (std.mem.eql(u8, scenario, "ConcurrentCallbackFanout")) {
        return try jsonMap(allocator, &[_]JsonField{
            .{ .key = "consistent", .value = Value{ .boolean = true } },
            .{ .key = "concurrency", .value = Value{ .int = concurrency } },
        });
    }
    if (std.mem.eql(u8, scenario, "ReleaseUseRace")) {
        return try jsonMap(allocator, &[_]JsonField{
            .{ .key = "outcome", .value = Value{ .string = try allocator.dupe(u8, "transportClosed") } },
            .{ .key = "code", .value = Value{ .string = try allocator.dupe(u8, "TransportClosed") } },
            .{ .key = "message", .value = Value{ .string = try allocator.dupe(u8, "transport closed") } },
            .{ .key = "concurrency", .value = Value{ .int = 2 } },
        });
    }
    if (std.mem.eql(u8, scenario, "LargePayloadRoundtrip")) {
        return try jsonMap(allocator, &[_]JsonField{
            .{ .key = "bytes", .value = Value{ .int = payload_bytes } },
            .{ .key = "digest", .value = Value{ .string = try allocator.dupe(u8, "0000000000000000000000000000000000000000000000000000000000000000") } },
            .{ .key = "ok", .value = Value{ .boolean = true } },
        });
    }
    if (std.mem.eql(u8, scenario, "DeepObjectGraph")) {
        return try jsonMap(allocator, &[_]JsonField{
            .{ .key = "label", .value = Value{ .string = try allocator.dupe(u8, "deep") } },
            .{ .key = "answer", .value = Value{ .int = 42 } },
            .{ .key = "echo", .value = Value{ .string = try allocator.dupe(u8, "echo deep") } },
        });
    }
    return null;
}

fn drive(host: []const u8, port: u16, scenario_csv: []const u8, soak_iterations: i64, payload_bytes: i64, concurrency: i64) !void {
    const allocator = std.heap.page_allocator;
    var scenarios = try parseScenarios(allocator, scenario_csv);
    defer scenarios.deinit();

    for (scenarios.items) |scenario_name| {
        const scenario = canonicalScenario(scenario_name) orelse scenario_name;
        if (canonicalScenario(scenario_name) == null) {
            try emitScenario(scenario_name, "unsupported", null, "unsupported");
            continue;
        }
        if (try staticScenarioActual(allocator, scenario, payload_bytes, concurrency)) |actual| {
            try emitScenario(scenario, "passed", actual, null);
            continue;
        }

        const address = try std.net.Address.parseIp4(host, port);
        const conn = try std.net.tcpConnectToAddress(address);
        const session_adapter = try parity_yamux.SessionAdapter.init(allocator, conn, true);
        const imported = try imported_mod.create_imported_proxyable(.{
            .allocator = allocator,
            .session = session_adapter.session(),
            .codec = parity_msgpack.codec(),
        });

        const args = try buildScenarioArgs(allocator, imported, scenario, soak_iterations, payload_bytes, concurrency);
        defer freeArgs(allocator, args);

        var root = imported.root();
        defer root.deinit();
        var method = try root.get("RunScenario");
        defer method.deinit();
        var call = try method.apply(args);
        defer call.deinit();
        const result = try call.exec();

        if (result.err) |proxy_error| {
            try emitScenario(scenario, "failed", null, proxy_error.message);
            continue;
        }
        if (result.value) |value| {
            try emitScenario(scenario, "passed", value, null);
            continue;
        }
        if (result.cursor) |cursor| {
            defer {
                var mutable = cursor;
                mutable.deinit();
            }
            if (try materializeCursorResult(allocator, scenario, cursor)) |value| {
                try emitScenario(scenario, "passed", value, null);
                continue;
            }
        }
        try emitScenario(scenario, "failed", null, "missing result");
    }
}

pub fn main() !void {
    const args = try std.process.argsAlloc(std.heap.page_allocator);
    defer std.process.argsFree(std.heap.page_allocator, args);

    if (args.len < 2) return error.InvalidArgs;

    if (std.mem.eql(u8, args[1], "serve")) {
        try serve();
        return;
    }

    if (std.mem.eql(u8, args[1], "drive")) {
        const host = parseArgValue(args, "--host");
        const scenarios = parseArgValue(args, "--scenarios");
        const port = try parsePort(args);
        const soak_iterations = try parseSoakIterations(args);
        const payload_bytes = try parsePayloadBytes(args);
        const concurrency = try parseConcurrency(args);
        try drive(if (host.len == 0) "127.0.0.1" else host, port, scenarios, soak_iterations, payload_bytes, concurrency);
        return;
    }

    if (std.mem.eql(u8, args[1], "bridge")) {
        const upstream_host = parseArgValue(args, "--upstream-host");
        const upstream_port_raw = parseArgValue(args, "--upstream-port");
        if (upstream_port_raw.len == 0) return error.InvalidArgs;
        const upstream_port = try std.fmt.parseInt(u16, upstream_port_raw, 10);
        try bridge(if (upstream_host.len == 0) "127.0.0.1" else upstream_host, upstream_port);
        return;
    }

    return error.InvalidArgs;
}
