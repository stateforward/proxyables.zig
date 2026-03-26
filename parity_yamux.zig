const std = @import("std");
const transport = @import("transport.zig");

const PROTOCOL_VERSION: u8 = 0;
const TYPE_DATA: u8 = 0;
const TYPE_WINDOW_UPDATE: u8 = 1;
const TYPE_PING: u8 = 2;
const TYPE_GO_AWAY: u8 = 3;
const FLAG_SYN: u16 = 0x1;
const FLAG_ACK: u16 = 0x2;
const FLAG_FIN: u16 = 0x4;
const FLAG_RST: u16 = 0x8;
const HEADER_LENGTH: usize = 12;
const WINDOW_SIZE: u32 = 256 * 1024;

pub const SessionAdapter = struct {
    allocator: std.mem.Allocator,
    conn: std.net.Stream,
    next_stream_id: u32,
    running: bool = true,

    read_thread_started: bool = false,
    mutex: std.Thread.Mutex = .{},
    accept_cond: std.Thread.Condition = .{},
    write_mutex: std.Thread.Mutex = .{},
    streams: std.AutoHashMapUnmanaged(u32, *YamuxStream) = .{},
    accept_queue: std.array_list.Managed(*YamuxStream),

    pub fn init(allocator: std.mem.Allocator, conn: std.net.Stream, is_client: bool) !*SessionAdapter {
        const adapter = try allocator.create(SessionAdapter);
        adapter.* = .{
            .allocator = allocator,
            .conn = conn,
            .next_stream_id = if (is_client) 1 else 2,
            .accept_queue = std.array_list.Managed(*YamuxStream).init(allocator),
        };

        const reader = try std.Thread.spawn(.{}, readLoop, .{adapter});
        reader.detach();
        adapter.read_thread_started = true;
        return adapter;
    }

    pub fn session(self: *SessionAdapter) transport.Session {
        return .{ .ctx = self, .vtable = &session_vtable };
    }

    pub fn close(self: *SessionAdapter) void {
        self.mutex.lock();
        if (!self.running) {
            self.mutex.unlock();
            return;
        }
        self.running = false;
        var iterator = self.streams.valueIterator();
        while (iterator.next()) |stream| {
            stream.*.onFin();
        }
        self.accept_cond.broadcast();
        self.mutex.unlock();

        self.conn.close();
    }

    fn open(ctx: *anyopaque) anyerror!transport.Stream {
        const self: *SessionAdapter = @ptrCast(@alignCast(ctx));
        return (try self.openStream()).stream();
    }

    fn accept(ctx: *anyopaque) anyerror!transport.Stream {
        const self: *SessionAdapter = @ptrCast(@alignCast(ctx));
        return (try self.acceptStream()).stream();
    }

    fn closeSession(ctx: *anyopaque) void {
        const self: *SessionAdapter = @ptrCast(@alignCast(ctx));
        self.close();
    }

    fn openStream(self: *SessionAdapter) !*YamuxStream {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.running) {
            return error.EndOfStream;
        }

        const stream_id = self.next_stream_id;
        self.next_stream_id += 2;

        const yamux_stream = try YamuxStream.init(self.allocator, self, stream_id);
        try self.streams.put(self.allocator, stream_id, yamux_stream);

        self.mutex.unlock();
        defer self.mutex.lock();
        try self.writeControlFrame(TYPE_WINDOW_UPDATE, FLAG_SYN, stream_id, WINDOW_SIZE);
        return yamux_stream;
    }

    fn acceptStream(self: *SessionAdapter) !*YamuxStream {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.accept_queue.items.len == 0 and self.running) {
            self.accept_cond.wait(&self.mutex);
        }

        if (self.accept_queue.items.len == 0) {
            return error.EndOfStream;
        }

        return self.accept_queue.orderedRemove(0);
    }

    fn writeFrame(self: *SessionAdapter, frame_type: u8, flags: u16, stream_id: u32, payload: []const u8) !void {
        self.mutex.lock();
        const running = self.running;
        self.mutex.unlock();
        if (!running) return error.EndOfStream;

        var header: [HEADER_LENGTH]u8 = undefined;
        header[0] = PROTOCOL_VERSION;
        header[1] = frame_type;
        std.mem.writeInt(u16, @ptrCast(header[2..4]), flags, .big);
        std.mem.writeInt(u32, @ptrCast(header[4..8]), stream_id, .big);
        std.mem.writeInt(u32, @ptrCast(header[8..12]), @intCast(payload.len), .big);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        try self.conn.writeAll(&header);
        if (payload.len > 0) {
            try self.conn.writeAll(payload);
        }
    }

    fn writeControlFrame(self: *SessionAdapter, frame_type: u8, flags: u16, stream_id: u32, length: u32) !void {
        self.mutex.lock();
        const running = self.running;
        self.mutex.unlock();
        if (!running) return error.EndOfStream;

        var header: [HEADER_LENGTH]u8 = undefined;
        header[0] = PROTOCOL_VERSION;
        header[1] = frame_type;
        std.mem.writeInt(u16, @ptrCast(header[2..4]), flags, .big);
        std.mem.writeInt(u32, @ptrCast(header[4..8]), stream_id, .big);
        std.mem.writeInt(u32, @ptrCast(header[8..12]), length, .big);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try self.conn.writeAll(&header);
    }

    fn readLoop(self: *SessionAdapter) void {
        var header: [HEADER_LENGTH]u8 = undefined;

        while (self.readExact(&header)) {
            const version = header[0];
            if (version != PROTOCOL_VERSION) break;

            const frame_type = header[1];
            const flags = std.mem.readInt(u16, header[2..4], .big);
            const stream_id = std.mem.readInt(u32, header[4..8], .big);
            const length = std.mem.readInt(u32, header[8..12], .big);

            var payload: []u8 = &.{};
            if (frame_type == TYPE_DATA and length > 0) {
                payload = self.allocator.alloc(u8, length) catch break;
                errdefer self.allocator.free(payload);
                if (!self.readExact(payload)) break;
            }

            self.handleFrame(frame_type, flags, stream_id, length, payload);
        }

        self.close();
    }

    fn readExact(self: *SessionAdapter, buffer: []u8) bool {
        var offset: usize = 0;
        while (offset < buffer.len) {
            const read = self.conn.read(buffer[offset..]) catch return false;
            if (read == 0) return false;
            offset += read;
        }
        return true;
    }

    fn handleFrame(self: *SessionAdapter, frame_type: u8, flags: u16, stream_id: u32, length: u32, payload: []u8) void {
        if (frame_type == TYPE_PING) {
            if ((flags & FLAG_SYN) != 0) {
                self.writeControlFrame(TYPE_PING, FLAG_ACK, 0, length) catch {};
            }
            if (payload.len > 0) self.allocator.free(payload);
            return;
        }
        if (frame_type == TYPE_GO_AWAY) {
            if (payload.len > 0) self.allocator.free(payload);
            self.close();
            return;
        }

        var stream_ptr: ?*YamuxStream = null;
        var should_ack_open = false;
        var locked = false;

        self.mutex.lock();
        locked = true;
        defer if (locked) self.mutex.unlock();

        if ((flags & FLAG_SYN) != 0 and !self.streams.contains(stream_id)) {
            const yamux_stream = YamuxStream.init(self.allocator, self, stream_id) catch return;
            self.streams.put(self.allocator, stream_id, yamux_stream) catch return;
            self.accept_queue.append(yamux_stream) catch return;
            self.accept_cond.signal();
            should_ack_open = true;
        }

        stream_ptr = self.streams.get(stream_id);
        if (stream_ptr == null) {
            if (payload.len > 0) self.allocator.free(payload);
            return;
        }

        if (frame_type == TYPE_DATA and payload.len > 0) {
            stream_ptr.?.feedData(payload) catch {
                self.allocator.free(payload);
            };
        } else if (payload.len > 0) {
            self.allocator.free(payload);
        }

        if ((flags & (FLAG_FIN | FLAG_RST)) != 0) {
            stream_ptr.?.onFin();
        }
        self.mutex.unlock();
        locked = false;

        if (should_ack_open) {
            self.writeControlFrame(TYPE_WINDOW_UPDATE, FLAG_ACK, stream_id, WINDOW_SIZE) catch {};
        }
    }

    const session_vtable = transport.Session.VTable{
        .open = open,
        .accept = accept,
        .close = closeSession,
    };
};

const YamuxStream = struct {
    allocator: std.mem.Allocator,
    session: *SessionAdapter,
    stream_id: u32,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    buffers: std.array_list.Managed([]u8),
    head_offset: usize = 0,
    remote_closed: bool = false,
    local_closed: bool = false,

    fn init(allocator: std.mem.Allocator, session: *SessionAdapter, stream_id: u32) !*YamuxStream {
        const yamux_stream = try allocator.create(YamuxStream);
        yamux_stream.* = .{
            .allocator = allocator,
            .session = session,
            .stream_id = stream_id,
            .buffers = std.array_list.Managed([]u8).init(allocator),
        };
        return yamux_stream;
    }

    fn stream(self: *YamuxStream) transport.Stream {
        return .{ .ctx = self, .vtable = &stream_vtable };
    }

    fn read(ctx: *anyopaque, out: []u8) anyerror!usize {
        const self: *YamuxStream = @ptrCast(@alignCast(ctx));
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.buffers.items.len == 0 and !self.remote_closed) {
            self.cond.wait(&self.mutex);
        }

        if (self.buffers.items.len == 0) {
            return 0;
        }

        const head = self.buffers.items[0];
        const remaining = head[self.head_offset..];
        const count = @min(out.len, remaining.len);
        @memcpy(out[0..count], remaining[0..count]);
        self.head_offset += count;

        if (self.head_offset >= head.len) {
            self.allocator.free(head);
            _ = self.buffers.orderedRemove(0);
            self.head_offset = 0;
        }

        return count;
    }

    fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
        const self: *YamuxStream = @ptrCast(@alignCast(ctx));
        self.mutex.lock();
        const local_closed = self.local_closed;
        const remote_closed = self.remote_closed;
        self.mutex.unlock();

        if (local_closed or remote_closed) {
            return error.BrokenPipe;
        }

        try self.session.writeFrame(TYPE_DATA, 0, self.stream_id, data);
    }

    fn close(ctx: *anyopaque) void {
        const self: *YamuxStream = @ptrCast(@alignCast(ctx));
        self.mutex.lock();
        if (self.local_closed) {
            self.mutex.unlock();
            return;
        }
        self.local_closed = true;
        self.mutex.unlock();

        self.session.mutex.lock();
        const running = self.session.running;
        self.session.mutex.unlock();
        if (!running) return;

        self.session.writeControlFrame(TYPE_WINDOW_UPDATE, FLAG_FIN, self.stream_id, 0) catch {};
    }

    fn feedData(self: *YamuxStream, payload: []u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.buffers.append(payload);
        self.cond.signal();
    }

    fn onFin(self: *YamuxStream) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.remote_closed = true;
        self.cond.broadcast();
    }

    const stream_vtable = transport.Stream.VTable{
        .read = read,
        .write = write,
        .close = close,
    };
};
