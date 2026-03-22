const std = @import("std");
const transport = @import("transport.zig");

pub const StreamPool = struct {
    allocator: std.mem.Allocator,
    session: transport.Session,
    max: usize,
    reuse: bool,

    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    open: usize = 0,
    idle: std.ArrayList(transport.Stream),
    shutdown: bool = false,

    pub fn init(allocator: std.mem.Allocator, session: transport.Session, max: usize, reuse: bool) StreamPool {
        return .{
            .allocator = allocator,
            .session = session,
            .max = if (max < 1) 1 else max,
            .reuse = reuse,
            .idle = std.ArrayList(transport.Stream).init(allocator),
        };
    }

    pub fn deinit(self: *StreamPool) void {
        self.close();
        self.idle.deinit();
    }

    pub fn acquire(self: *StreamPool) !transport.Stream {
        self.mutex.lock();
        while (true) {
            if (self.shutdown) {
                self.mutex.unlock();
                return error.StreamPoolClosed;
            }
            if (self.idle.items.len > 0) {
                const stream = self.idle.pop();
                self.mutex.unlock();
                return stream;
            }
            if (self.open < self.max) {
                self.open += 1;
                break;
            }
            self.cond.wait(&self.mutex);
        }
        self.mutex.unlock();

        const stream_result = self.session.open();

        self.mutex.lock();
        defer self.mutex.unlock();
        if (stream_result) |stream| {
            return stream;
        } else |err| {
            self.open -= 1;
            self.cond.signal();
            return err;
        }
    }

    pub fn release(self: *StreamPool, stream: transport.Stream) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.shutdown) {
            self.open = if (self.open > 0) self.open - 1 else 0;
            stream.close();
            return;
        }

        if (!self.reuse) {
            self.open = if (self.open > 0) self.open - 1 else 0;
            stream.close();
            self.cond.signal();
            return;
        }

        _ = self.idle.append(stream) catch {
            self.open = if (self.open > 0) self.open - 1 else 0;
            stream.close();
            return;
        };
        self.cond.signal();
    }

    pub fn close(self: *StreamPool) void {
        self.mutex.lock();
        if (self.shutdown) {
            self.mutex.unlock();
            return;
        }
        self.shutdown = true;
        var idle = self.idle.toOwnedSlice() catch |err| {
            _ = err;
            self.idle.clearRetainingCapacity();
            self.mutex.unlock();
            return;
        };
        self.idle.clearRetainingCapacity();
        self.mutex.unlock();

        for (idle) |stream| stream.close();
        self.allocator.free(idle);
        self.cond.broadcast();
    }
};
