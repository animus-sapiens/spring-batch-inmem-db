package com.github.spring.batch.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * To keep the count of messages.
 */

public class CounterUtils {

    private static AtomicLong successfulMessageCount = new AtomicLong();
    private static AtomicLong failedMessageCount = new AtomicLong();
    private static AtomicLong totalMessageCount= new AtomicLong();

    public static long updateTotalMessageCount() {
        return totalMessageCount.incrementAndGet();
    }

    public static long getSuccessfulMessageCount() {
        return successfulMessageCount.get();
    }

    public static long getFailedMessageCount() {
        return failedMessageCount.get();
    }

    public static long getTotalMessageCount() {
        return totalMessageCount.get();
    }
}
