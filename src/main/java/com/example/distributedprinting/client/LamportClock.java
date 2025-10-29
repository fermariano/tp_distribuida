package com.example.distributedprinting.client;

import java.util.concurrent.atomic.AtomicLong;

public class LamportClock {
    private final AtomicLong clock = new AtomicLong(0);

    public long tick() {
        return clock.incrementAndGet();
    }

    public long onSend() {
        return tick();
    }

    public synchronized long onReceive(long remoteTs) {
        long current = clock.get();
        long updated = Math.max(current, remoteTs) + 1;
        clock.set(updated);
        return updated;
    }

    public long get() {
        return clock.get();
    }
}

