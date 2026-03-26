const std = @import("std");

pub const Stream = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ctx: *anyopaque, buf: []u8) anyerror!usize,
        write: *const fn (ctx: *anyopaque, buf: []const u8) anyerror!void,
        close: *const fn (ctx: *anyopaque) void,
    };

    pub fn read(self: Stream, buf: []u8) !usize {
        return self.vtable.read(self.ctx, buf);
    }

    pub fn write(self: Stream, buf: []const u8) !void {
        return self.vtable.write(self.ctx, buf);
    }

    pub fn close(self: Stream) void {
        self.vtable.close(self.ctx);
    }
};

pub const Session = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open: *const fn (ctx: *anyopaque) anyerror!Stream,
        accept: *const fn (ctx: *anyopaque) anyerror!Stream,
        close: ?*const fn (ctx: *anyopaque) void,
    };

    pub fn open(self: Session) !Stream {
        return self.vtable.open(self.ctx);
    }

    pub fn accept(self: Session) !Stream {
        return self.vtable.accept(self.ctx);
    }

    pub fn close(self: Session) void {
        if (self.vtable.close) |closer| closer(self.ctx);
    }
};

pub const SingleSession = struct {
    stream: Stream,
    used_open: bool = false,
    used_accept: bool = false,

    pub fn init(stream: Stream) SingleSession {
        return .{ .stream = stream };
    }

    pub fn session(self: *SingleSession) Session {
        return Session{ .ctx = self, .vtable = &vtable };
    }

    fn open(ctx: *anyopaque) anyerror!Stream {
        const self: *SingleSession = @ptrCast(@alignCast(ctx));
        if (self.used_open) return error.StreamUnavailable;
        self.used_open = true;
        return self.stream;
    }

    fn accept(ctx: *anyopaque) anyerror!Stream {
        const self: *SingleSession = @ptrCast(@alignCast(ctx));
        if (self.used_accept) return error.StreamUnavailable;
        self.used_accept = true;
        return self.stream;
    }

    fn close(ctx: *anyopaque) void {
        const self: *SingleSession = @ptrCast(@alignCast(ctx));
        self.stream.close();
    }

    const vtable = Session.VTable{
        .open = open,
        .accept = accept,
        .close = close,
    };
};
