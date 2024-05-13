package com.github.spring.batch.listener;

import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.CompositeJobExecutionListener;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Takes all the records that were skipped and writes out to a unique file.
 */
public class ErrorRecordsWriteListener extends CompositeJobExecutionListener {
	
	private static final Logger log = LoggerFactory.getLogger(ErrorRecordsWriteListener.class);
	
	private String skippedRecordsFileNameSeperator = "_";
	
	private String skippedRecordsFileNameBase = "skipped";
	
	private String skippedRecordsFileNameType = ".csv";
	
	private ConcurrentLinkedQueue<String> skippedRecordsQueue;
	
	public ErrorRecordsWriteListener (ConcurrentLinkedQueue<String> inSkippedRecordsQueue) {
		skippedRecordsQueue = inSkippedRecordsQueue;
	}
	
    @Override   
    public void afterJob(JobExecution jobExecution) {        	
    	// exist status is FAILED if the skip limit is exceeded
    	// the skip listener is not called and ids are not saved when skip limit exceeded
    	// since the business assumption is catastrophic failure if skip limit reached,
    	// do not print out the skip file
    	
    	if(ExitStatus.FAILED.getExitCode().equals(jobExecution.getExitStatus().getExitCode())) {
    		log.info("application exit status is {}, skip list may be incomplete, do not write file", jobExecution.getExitStatus().getExitCode());
    		return;    		
    	}
    	    	
    	if(skippedRecordsQueue==null || skippedRecordsQueue.size()<1) {
    		log.info("no records skipped");
    		return;
    	}

        	
		// create a unique filename based on the current date
		// filename includes the message type and current date
		LocalDateTime now = LocalDateTime.now();
		String fileName = skippedRecordsFileNameBase
        			+ skippedRecordsFileNameSeperator
        			+ now.getYear()
        			+ now.getMonthOfYear()
        			+ now.getDayOfMonth()
        			+ now.getHourOfDay()
        			+ now.getMinuteOfHour()
        			+ skippedRecordsFileNameType;
        	
		log.info("writing error record file {}", fileName);
			
		for(String recordId : skippedRecordsQueue) {
			System.out.println("Skipped record with id=" + recordId);
		}
								
	}

}
