package com.antigravity.concurrency.lab;

/**
 * Configuration parameters for the concurrency engines.
 */
public final class EngineConfig {
    private final int simulatedIoMicros;
    private final int bufferCapacity;
    private final int crdtPartitions;
    private final int maxBatchSize;

    private EngineConfig(Builder builder) {
        this.simulatedIoMicros = builder.simulatedIoMicros;
        this.bufferCapacity = builder.bufferCapacity;
        this.crdtPartitions = builder.crdtPartitions;
        this.maxBatchSize = builder.maxBatchSize;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static EngineConfig defaultInMemory() {
        return newBuilder().build();
    }

    public static EngineConfig withSimulatedIo(int ioMicros) {
        return newBuilder().simulatedIoMicros(ioMicros).build();
    }

    public int getSimulatedIoMicros() {
        return simulatedIoMicros;
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public int getCrdtPartitions() {
        return crdtPartitions;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public static final class Builder {
        private int simulatedIoMicros = 0;
        private int bufferCapacity = 65536; // 64K slots
        private int crdtPartitions = 16;
        private int maxBatchSize = 4096;

        public Builder simulatedIoMicros(int simulatedIoMicros) {
            this.simulatedIoMicros = simulatedIoMicros;
            return this;
        }

        public Builder bufferCapacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder crdtPartitions(int crdtPartitions) {
            this.crdtPartitions = crdtPartitions;
            return this;
        }

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public EngineConfig build() {
            return new EngineConfig(this);
        }
    }
}
