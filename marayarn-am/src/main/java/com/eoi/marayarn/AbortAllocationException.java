package com.eoi.marayarn;

/**
 * Throw this exception when the total number of failed containers exceed threshold
 */
public class AbortAllocationException extends Exception {

    public final int threshold;

    public AbortAllocationException(int threshold) {
        this.threshold = threshold;
    }

    public int getThreshold() {
        return threshold;
    }
}
