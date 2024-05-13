package com.github.spring.batch.listener;

import com.github.spring.batch.util.CounterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.CompositeJobExecutionListener;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Logs out the counts from the job.
 */
public class JobLoggingCountsListener extends CompositeJobExecutionListener {
	private static final Logger log = LoggerFactory.getLogger(JobLoggingCountsListener.class);
	
    @Override
    public void afterJob(JobExecution jobExecution) {
        Long jobExecId = jobExecution.getId();
        String jobStatus = jobExecution.getStatus().toString();
        int jobReadCount = 0;
        int jobWriteCount = 0;
        int jobSkipCount = 0;

           long jobMs = (jobExecution.getEndTime() != null && jobExecution.getStartTime() != null) 
           			? ZonedDateTime.of(jobExecution.getEndTime(), ZoneId.systemDefault()).toInstant().toEpochMilli() -
                   ZonedDateTime.of(jobExecution.getStartTime(), ZoneId.systemDefault()).toInstant().toEpochMilli()
           		    : 0;
              	            
           for(StepExecution stepExecution : jobExecution.getStepExecutions()) {    	            	   	            	   	            	
                    long stepReadCount =  stepExecution.getReadCount();
                    long stepWriteCount = stepExecution.getWriteCount();
                    long stepSkipCount = stepExecution.getSkipCount();
                    long stepMs = 0;
                    
                    if (stepExecution.getEndTime() != null && stepExecution.getStartTime() != null) {
                        stepMs = ZonedDateTime.of(stepExecution.getEndTime(), ZoneId.systemDefault()).toInstant().toEpochMilli() -
                                ZonedDateTime.of(stepExecution.getStartTime(), ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                    
                    jobReadCount += stepReadCount;
                    jobWriteCount += stepWriteCount;
                    jobSkipCount += stepSkipCount;
                    
                    //log each step in the job
    	            log.info("Step {} Ended. jobExecId={}, status={}"
    	            		+ ", readCount={}, writeCount={}, skipCount={}"
    	            		+ ", stepExecMillis={}", 
    	            		stepExecution.getStepName(), jobExecId, stepExecution.getStatus().toString(),
    	            		stepReadCount, stepWriteCount, stepSkipCount,
    	            		stepMs);    	                    
            }

            //logs the job totals 
            log.info("Job {} Ended. jobExecId={}, status={}"
            		+ ", readCount={}, writeCount={}, skipCount={}"
            		+ ", jobExecMillis={}", 
            		jobExecution.getJobInstance().getJobName(), jobExecId, jobStatus,
            		jobReadCount, jobWriteCount, jobSkipCount, 
            		jobMs);

           log.info("totalMessageCount={}, successfulMessageCount={}, failedMessageCount={}", CounterUtils.getTotalMessageCount(), CounterUtils.getSuccessfulMessageCount(), CounterUtils.getFailedMessageCount());

    }
}