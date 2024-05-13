package com.github.spring.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Handles records skipped during the batch job.
 */
public class RecordSkipListener implements SkipListener {
	
	private static final Logger log = LoggerFactory.getLogger(RecordSkipListener.class);
	
	private ConcurrentLinkedQueue<String> skippedRecordsQueue;
	
	public RecordSkipListener(ConcurrentLinkedQueue<String> inSkippedRecordsQueue) {
		skippedRecordsQueue = inSkippedRecordsQueue;
	}
}
