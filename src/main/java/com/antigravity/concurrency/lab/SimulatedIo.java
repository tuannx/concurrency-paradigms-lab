package com.antigravity.concurrency.lab;

import java.util.concurrent.locks.LockSupport;

/**
 * Utility to simulate persistent I/O (WAL append, disk fsync, or network RPC).
 */
public final class SimulatedIo {
    private SimulatedIo() {}

    /**
     * Simulates an I/O operation of the given duration in microseconds.
     *
     * @param micros duration in microseconds (0 means pure in-memory)
     */
    public static void execute(int micros) {
        if (micros <= 0) {
            return;
        }
        // For microsecond scale, use parkNanos to allow other threads or spin
        if (micros >= 100) {
            LockSupport.parkNanos(micros * 1000L);
        } else {
            // High-precision spin-wait for sub-100 microsecond simulated writes
            long deadline = System.nanoTime() + (micros * 1000L);
            while (System.nanoTime() < deadline) {
                // busy spin
            }
        }
    }
}
