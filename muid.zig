const std = @import("std");

const EPOCH: u64 = 1700000000000;
const TIMESTAMP_BITS: u64 = 41;
const MACHINE_ID_BITS: u64 = 14;
const COUNTER_BITS: u64 = 9;
const MAX_MACHINE_ID: u64 = (1 << MACHINE_ID_BITS) - 1;
const MAX_COUNTER: u64 = (1 << COUNTER_BITS) - 1;

const GeneratorState = struct {
    last_timestamp: u64 = 0,
    counter: u64 = 0,
    machine_id: u64 = 0,
    initialized: bool = false,
};

var mutex = std.Thread.Mutex{};
var state = GeneratorState{};

pub fn make(allocator: std.mem.Allocator) ![]const u8 {
    mutex.lock();
    defer mutex.unlock();

    if (!state.initialized) {
        state.machine_id = init_machine_id() & MAX_MACHINE_ID;
        state.initialized = true;
    }

    var now_ms: u64 = @intCast(u64, std.time.milliTimestamp());
    if (now_ms < EPOCH) now_ms = EPOCH;
    var current_timestamp = now_ms - EPOCH;

    if (current_timestamp < state.last_timestamp) {
        current_timestamp = state.last_timestamp;
    }

    if (current_timestamp == state.last_timestamp) {
        state.counter = (state.counter + 1) & MAX_COUNTER;
        if (state.counter == 0) {
            // Wait for next millisecond
            while (current_timestamp == state.last_timestamp) {
                now_ms = @intCast(u64, std.time.milliTimestamp());
                if (now_ms < EPOCH) now_ms = EPOCH;
                current_timestamp = now_ms - EPOCH;
            }
        }
    } else {
        state.counter = 0;
    }

    state.last_timestamp = current_timestamp;

    const id_int = (current_timestamp << (MACHINE_ID_BITS + COUNTER_BITS)) |
        (state.machine_id << COUNTER_BITS) |
        state.counter;

    return to_base32(allocator, id_int);
}

fn init_machine_id() u64 {
    var buf: [256]u8 = undefined;
    var hostname: []const u8 = "";
    if (std.os.gethostname(&buf)) |len| {
        hostname = buf[0..len];
    } else |_| {
        hostname = "";
    }

    if (hostname.len == 0) {
        var rng = std.rand.DefaultPrng.init(@intCast(u64, std.time.nanoTimestamp()));
        return rng.random().int(u64) & MAX_MACHINE_ID;
    }

    var hasher = std.hash.Wyhash.init(0);
    hasher.update(hostname);
    return hasher.final() & MAX_MACHINE_ID;
}

fn to_base32(allocator: std.mem.Allocator, num: u64) ![]const u8 {
    const alphabet = "0123456789abcdefghijklmnopqrstuv";
    if (num == 0) return allocator.dupe(u8, "0");

    var buf: [32]u8 = undefined;
    var i: usize = 0;
    var n = num;
    while (n > 0) : (n /= 32) {
        buf[i] = alphabet[@intCast(usize, n % 32)];
        i += 1;
    }

    var out = try allocator.alloc(u8, i);
    var j: usize = 0;
    while (j < i) : (j += 1) {
        out[j] = buf[i - 1 - j];
    }
    return out;
}
