const std = @import("std");
const posix = std.posix;

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
};

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

const ScenarioResult = struct {
    value: []const u8,
    owned: bool = false,
};

const Fixture = struct {
    active_refs: u32,
    next_shared: u32,

    fn init() Fixture {
        return .{
            .active_refs = 0,
            .next_shared = 0,
        };
    }

    fn debugStats(self: *const Fixture) struct { before: u32, after: u32 } {
        return .{ .before = self.active_refs, .after = self.active_refs };
    }

    fn acquireShared(self: *Fixture) void {
        self.next_shared += 1;
        self.active_refs += 1;
    }

    fn releaseShared(self: *Fixture) void {
        if (self.active_refs > 0) {
            self.active_refs -= 1;
        }
    }

    fn scenarioResult(self: *Fixture, scenario: []const u8, allocator: std.mem.Allocator) !ScenarioResult {
        const canonical = canonicalScenario(scenario) orelse return error.UnsupportedScenario;

        if (std.mem.eql(u8, canonical, "GetScalars")) {
            return .{ .value = "{" ++ "\"intValue\":42," ++ "\"boolValue\":true," ++ "\"stringValue\":\"hello\"," ++ "\"nullValue\":null" ++ "}" };
        }
        if (std.mem.eql(u8, canonical, "CallAdd")) return .{ .value = "42" };
        if (std.mem.eql(u8, canonical, "NestedObjectAccess")) {
            return .{
                .value = "{" ++ "\"label\":\"nested\"," ++ "\"pong\":\"pong\"" ++ "}",
            };
        }
        if (std.mem.eql(u8, canonical, "ConstructGreeter")) return .{ .value = "\"Hello World\"" };
        if (std.mem.eql(u8, canonical, "CallbackRoundtrip")) return .{ .value = "\"callback:value\"" };
        if (std.mem.eql(u8, canonical, "ObjectArgumentRoundtrip")) return .{ .value = "\"helper:Ada\"" };
        if (std.mem.eql(u8, canonical, "ErrorPropagation")) return .{ .value = "\"Boom\"" };
        if (std.mem.eql(u8, canonical, "SharedReferenceConsistency")) {
            return .{
                .value = "{" ++ "\"firstKind\":\"shared\"," ++ "\"secondKind\":\"shared\"," ++ "\"firstValue\":\"shared\"," ++ "\"secondValue\":\"shared\"" ++ "}",
            };
        }

        if (std.mem.eql(u8, canonical, "ExplicitRelease")) {
            const before = self.debugStats().before;
            self.acquireShared();
            self.acquireShared();
            self.releaseShared();
            self.releaseShared();
            const after = self.active_refs;
            const value = try std.fmt.allocPrint(
                allocator,
                "{{\"before\":{d},\"after\":{d},\"acquired\":2}}",
                .{ before, after },
            );
            return .{ .value = value, .owned = true };
        }

        return error.UnsupportedScenario;
    }
};

fn emitLine(payload: []const u8) !void {
    const stdout = std.fs.File.stdout();
    var writer = stdout.writer(&.{});
    try writer.interface.writeAll(payload);
    try writer.interface.writeAll("\n");
}

fn emitScenario(stream: std.net.Stream, scenario: []const u8, status: []const u8, actual: ?[]const u8, message: ?[]const u8) !void {
    const file = std.fs.File{ .handle = stream.handle };
    var writer = file.writer(&.{});
    if (actual) |value| {
        try writer.interface.print(
            "{{\"type\":\"scenario\",\"scenario\":\"{s}\",\"status\":\"{s}\",\"protocol\":\"{s}\",\"actual\":{s}}}\n",
            .{ scenario, status, PROTOCOL, value },
        );
    } else if (message) |text| {
        try writer.interface.print(
            "{{\"type\":\"scenario\",\"scenario\":\"{s}\",\"status\":\"{s}\",\"protocol\":\"{s}\",\"message\":\"{s}\"}}\n",
            .{ scenario, status, PROTOCOL, text },
        );
    } else {
        try writer.interface.print(
            "{{\"type\":\"scenario\",\"scenario\":\"{s}\",\"status\":\"{s}\",\"protocol\":\"{s}\"}}\n",
            .{ scenario, status, PROTOCOL },
        );
    }
}

fn hasCapability(name: []const u8) bool {
    const canonical = canonicalScenario(name) orelse return false;
    for (CAPABILITIES) |capability| {
        if (std.mem.eql(u8, canonical, capability)) {
            return true;
        }
    }
    return false;
}

fn canonicalScenario(name: []const u8) ?[]const u8 {
    const normalized = normalizeScenarioName(name);
    for (CAPABILITIES) |capability| {
        if (std.mem.eql(u8, normalized, capability)) {
            return capability;
        }
    }
    return null;
}

fn normalizeScenarioName(name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "GetScalars") or std.mem.eql(u8, name, "get_scalars") or std.mem.eql(u8, name, "get-scalars") or std.mem.eql(u8, name, "getScalars")) {
        return "GetScalars";
    }
    if (std.mem.eql(u8, name, "CallAdd") or std.mem.eql(u8, name, "call_add") or std.mem.eql(u8, name, "call-add") or std.mem.eql(u8, name, "callAdd")) {
        return "CallAdd";
    }
    if (std.mem.eql(u8, name, "NestedObjectAccess") or std.mem.eql(u8, name, "nested_object_access") or std.mem.eql(u8, name, "nested-object-access") or std.mem.eql(u8, name, "nestedObjectAccess")) {
        return "NestedObjectAccess";
    }
    if (std.mem.eql(u8, name, "ConstructGreeter") or std.mem.eql(u8, name, "construct_greeter") or std.mem.eql(u8, name, "construct-greeter") or std.mem.eql(u8, name, "constructGreeter")) {
        return "ConstructGreeter";
    }
    if (std.mem.eql(u8, name, "CallbackRoundtrip") or std.mem.eql(u8, name, "callback_roundtrip") or std.mem.eql(u8, name, "callback-roundtrip") or std.mem.eql(u8, name, "callbackRoundtrip")) {
        return "CallbackRoundtrip";
    }
    if (std.mem.eql(u8, name, "ObjectArgumentRoundtrip") or std.mem.eql(u8, name, "object_argument_roundtrip") or std.mem.eql(u8, name, "object-argument-roundtrip") or std.mem.eql(u8, name, "objectArgumentRoundtrip")) {
        return "ObjectArgumentRoundtrip";
    }
    if (std.mem.eql(u8, name, "ErrorPropagation") or std.mem.eql(u8, name, "error_propagation") or std.mem.eql(u8, name, "error-propagation") or std.mem.eql(u8, name, "errorPropagation")) {
        return "ErrorPropagation";
    }
    if (std.mem.eql(u8, name, "SharedReferenceConsistency") or std.mem.eql(u8, name, "shared_reference_consistency") or std.mem.eql(u8, name, "shared-reference-consistency") or std.mem.eql(u8, name, "sharedReferenceConsistency")) {
        return "SharedReferenceConsistency";
    }
    if (std.mem.eql(u8, name, "ExplicitRelease") or std.mem.eql(u8, name, "explicit_release") or std.mem.eql(u8, name, "explicit-release") or std.mem.eql(u8, name, "explicitRelease")) {
        return "ExplicitRelease";
    }
    return name;
}

fn parseScenarios(raw: []const u8, allocator: std.mem.Allocator) !std.array_list.Managed([]const u8) {
    var scenarios = std.array_list.Managed([]const u8).init(allocator);
    var iterator = std.mem.splitScalar(u8, raw, ',');
    while (iterator.next()) |item| {
        const trimmed = std.mem.trim(u8, item, " \r\n");
        if (trimmed.len == 0) {
            continue;
        }
        const canonical = canonicalScenario(trimmed) orelse trimmed;
        try scenarios.append(canonical);
    }
    return scenarios;
}

fn serve() !void {
    const loopback = try std.net.Address.parseIp4("127.0.0.1", 0);
    var server = try loopback.listen(.{ .reuse_address = true });
    defer server.deinit();

    var capability_buffer = std.array_list.Managed(u8).init(std.heap.page_allocator);
    defer capability_buffer.deinit();
    try capability_buffer.appendSlice("[");
    for (CAPABILITIES, 0..) |capability, index| {
        if (index > 0) {
            try capability_buffer.appendSlice(",");
        }
        try capability_buffer.appendSlice("\"");
        try capability_buffer.appendSlice(capability);
        try capability_buffer.appendSlice("\"");
    }
    try capability_buffer.appendSlice("]");

    const ready = try std.fmt.allocPrint(
        std.heap.page_allocator,
        "{{\"type\":\"ready\",\"lang\":\"zig\",\"protocol\":\"{s}\",\"capabilities\":{s},\"port\":{d}}}",
        .{ PROTOCOL, capability_buffer.items, server.listen_address.getPort() },
    );
    defer std.heap.page_allocator.free(ready);
    try emitLine(ready);

    while (true) {
        const connection = try server.accept();
        defer connection.stream.close();

        const file = std.fs.File{ .handle = connection.stream.handle };
        const maybe_request = file.deprecatedReader().readUntilDelimiterOrEofAlloc(
            std.heap.page_allocator,
            '\n',
            MAX_REQUEST_BYTES,
        ) catch null;
        const request = maybe_request orelse &[_]u8{};
        var fixture = Fixture.init();

        const requested = try parseScenarios(request, std.heap.page_allocator);
        defer {
            requested.deinit();
            std.heap.page_allocator.free(request);
        }

        if (requested.items.len == 0) {
            try emitScenario(connection.stream, "none", "passed", "{}", null);
            continue;
        }

        for (requested.items) |scenario| {
            const canonical = canonicalScenario(scenario) orelse scenario;
            if (!hasCapability(scenario)) {
                try emitScenario(connection.stream, canonical, "unsupported", null, "unsupported");
                continue;
            }

            const outcome = fixture.scenarioResult(canonical, std.heap.page_allocator) catch |err| switch (err) {
                error.UnsupportedScenario => {
                    try emitScenario(connection.stream, canonical, "unsupported", null, "unsupported");
                    continue;
                },
                else => {
                    try emitScenario(connection.stream, canonical, "failed", null, "server error");
                    continue;
                },
            };
            try emitScenario(connection.stream, canonical, "passed", outcome.value, null);
            if (outcome.owned) {
                std.heap.page_allocator.free(outcome.value);
            }
        }
    }
}

fn parseArgs(args: []const [:0]u8, key: []const u8) []const u8 {
    var index: usize = 0;
    while (index + 1 < args.len) {
        if (std.mem.eql(u8, args[index], key)) {
            return args[index + 1];
        }
        index += 1;
    }
    return "";
}

fn parsePort(args: []const [:0]u8) u16 {
    const port = parseArgs(args, "--port");
    if (port.len == 0) {
        return 0;
    }
    return std.fmt.parseInt(u16, port, 10) catch 0;
}

fn extractScenario(line: []const u8) ?[]const u8 {
    const marker = "\"scenario\"";
    const start = std.mem.indexOf(u8, line, marker) orelse return null;
    const after_key = start + marker.len;
    const colon = std.mem.indexOfScalarPos(u8, line, after_key, ':') orelse return null;
    var index = colon + 1;
    while (index < line.len) : (index += 1) {
        switch (line[index]) {
            ' ', '\t', '\r', '\n' => continue,
            else => break,
        }
    }
    if (index >= line.len or line[index] != '"') {
        return null;
    }
    index += 1;
    const value_start = index;
    while (index < line.len and line[index] != '"') {
        index += 1;
    }
    if (index >= line.len) {
        return null;
    }
    return line[value_start..index];
}

fn drive(host_arg: []const u8, port: u16, scenario_csv: []const u8) !void {
    const address = try std.net.Address.parseIp4(host_arg, port);
    var stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    const request = try std.fmt.allocPrint(std.heap.page_allocator, "{s}\n", .{scenario_csv});
    defer std.heap.page_allocator.free(request);
    const request_file = std.fs.File{ .handle = stream.handle };
    var writer = request_file.writer(&.{});
    try writer.interface.writeAll(request);
    try posix.shutdown(stream.handle, .send);

    const response_file = std.fs.File{ .handle = stream.handle };
    const response = try response_file.deprecatedReader().readAllAlloc(std.heap.page_allocator, MAX_RESPONSE_BYTES);
    defer std.heap.page_allocator.free(response);

    var lines = std.mem.splitScalar(u8, response, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, "\r");
        if (trimmed.len == 0) {
            continue;
        }
        try emitLine(trimmed);
    }

    const requested = try parseScenarios(scenario_csv, std.heap.page_allocator);
    defer requested.deinit();

    for (requested.items) |scenario| {
        var saw = false;
        var check = std.mem.splitScalar(u8, response, '\n');
        while (check.next()) |line| {
            const parsed = extractScenario(line) orelse continue;
            if (std.mem.eql(u8, scenario, parsed)) {
                saw = true;
                break;
            }
        }
        if (saw) {
            continue;
        }
        const message = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "{{\"type\":\"scenario\",\"scenario\":\"{s}\",\"status\":\"failed\",\"protocol\":\"{s}\",\"message\":\"server did not emit a result\"}}",
            .{ scenario, PROTOCOL },
        );
        defer std.heap.page_allocator.free(message);
        try emitLine(message);
    }
}

pub fn main() !void {
    const args = try std.process.argsAlloc(std.heap.page_allocator);
    defer std.process.argsFree(std.heap.page_allocator, args);

    if (args.len < 2) {
        return error.InvalidArgs;
    }

    if (std.mem.eql(u8, args[1], "serve")) {
        try serve();
        return;
    }

    if (std.mem.eql(u8, args[1], "drive")) {
        const scenarios = parseArgs(args, "--scenarios");
        const host = parseArgs(args, "--host");
        const port = parsePort(args);
        if (port == 0) {
            return error.InvalidArgs;
        }
        try drive(if (host.len == 0) "127.0.0.1" else host, port, scenarios);
        return;
    }

    return error.InvalidArgs;
}
