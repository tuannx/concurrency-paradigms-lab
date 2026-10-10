package com.antigravity.concurrency.lab;

/**
 * Configuration parameters for the concurrency engines.
 */
public final class EngineConfig {
    private final int simulatedIoMicros;
    private final int bufferCapacity;
    private final int stripeCount;
    private final int maxBatchSize;

    private EngineConfig(Builder builder) {
        this.simulatedIoMicros = builder.simulatedIoMicros;
        this.bufferCapacity = builder.bufferCapacity;
        this.stripeCount = builder.stripeCount;
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

    public int getStripeCount() {
        return stripeCount;
    }

    /**
     * @deprecated Use {@link #getStripeCount()}; the striped counter is not a CRDT.
     */
    @Deprecated
    public int getCrdtPartitions() {
        return stripeCount;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public static final class Builder {
        private int simulatedIoMicros = 0;
        private int bufferCapacity = 65536; // 64K slots
        private int stripeCount = 16;
        private int maxBatchSize = 4096;

        public Builder simulatedIoMicros(int simulatedIoMicros) {
            this.simulatedIoMicros = simulatedIoMicros;
            return this;
        }

        public Builder bufferCapacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder stripeCount(int stripeCount) {
            this.stripeCount = stripeCount;
            return this;
        }

        /**
         * @deprecated Use {@link #stripeCount(int)}; the striped counter is not a CRDT.
         */
        @Deprecated
        public Builder crdtPartitions(int crdtPartitions) {
            return stripeCount(crdtPartitions);
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
