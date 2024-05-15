package com.github.spring.batch;

import com.github.spring.batch.config.TestConfig;
import org.joda.time.LocalDateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import java.util.concurrent.ExecutionException;

import com.github.spring.batch.config.BatchConfig;

import static org.junit.Assert.assertEquals;
import static com.github.spring.batch.config.BatchConfig.PARAM_DATE;

@DisabledInAotMode
@ExtendWith(SpringExtension.class)
@SpringBatchTest
@EnableAutoConfiguration
@SpringJUnitConfig(classes = { BatchConfig.class, TestConfig.class })
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class })

public class BatchConfigTest {
    private final static int WAIT_TIME_MILS = 1000;
    private final static String PARAM_ACCESSOR = "key";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobParametersValidator mockValidator;

    @BeforeEach
    public void setup() throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        ((DefaultJobParametersValidator) mockValidator).setOptionalKeys(new String[0]);
        ((DefaultJobParametersValidator) mockValidator).setRequiredKeys(new String[0]);
    }

    private JobParameters defaultJobParameters() {
        JobParametersBuilder paramsBuilder = new JobParametersBuilder();
        paramsBuilder.addString(PARAM_DATE, LocalDateTime.now().toString());
        return paramsBuilder.toJobParameters();
    }

    @Test
    public void job_param_null_fail() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            JobParameters  params = new JobParameters();
            JobParametersBuilder paramsBuilder = new JobParametersBuilder(params).addString(PARAM_ACCESSOR, null);
            jobLauncherTestUtils.launchJob(paramsBuilder.toJobParameters());

        }, "IllegalArgumentException was expected");
    }

    @Test
    public void job_success() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());

        Thread.sleep(WAIT_TIME_MILS);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(BatchStatus.COMPLETED.toString(), jobExecution.getExitStatus().getExitCode());

        int jobSkipCount = 0;
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            jobSkipCount += stepExecution.getSkipCount();
        }

        assertEquals(0, jobSkipCount);
    }

}
