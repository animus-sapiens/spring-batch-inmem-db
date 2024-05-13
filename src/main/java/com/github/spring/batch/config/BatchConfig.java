package com.github.spring.batch.config;

import com.github.spring.batch.listener.ErrorRecordsWriteListener;
import com.github.spring.batch.listener.JobLoggingCountsListener;
import com.github.spring.batch.listener.RecordSkipListener;

import org.apache.commons.collections.KeyValue;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.listener.CompositeJobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

@Configuration
@EnableBatchProcessing(dataSourceRef = "dataSource", transactionManagerRef = "transactionManager")
/**
 * One place for defining all spring-batch configurations.
 */
public class BatchConfig {
    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);
    private static final String JOB_NAME = "processDataMesgFileJob";
    public static final String PARAM_DATE = "date";

    @Value("${data.mesg.job.chunk.size:10}")
    private int chunkSize;

    @Value("${batch.job.isolation.level:ISOLATION_SERIALIZABLE}")
    private String isolationLevel;

    /**
     * Job
     * @param jobRepository
     * @param validator
     * @param incrementor
     * @param processDataMesgFileStep
     * @param jobLoggingListener
     * @param errorRecordWriteListener
     * @return
     */
    @Bean
    @Qualifier("processDataMesgFileJob")
    public Job job(JobRepository jobRepository,
            @Qualifier("dataMesgParamsValidator") JobParametersValidator validator,
            JobParametersIncrementer incrementor,
            @Qualifier("processDataMesgFileStep") Step processDataMesgFileStep,
            @Qualifier("jobLoggingListener") CompositeJobExecutionListener jobLoggingListener,
            @Qualifier("errorRecordWriteListener") CompositeJobExecutionListener errorRecordWriteListener) {
        Job job = new JobBuilder(JOB_NAME, jobRepository)
                .validator(validator)
                .incrementer(incrementor)
                .listener(jobLoggingListener)
                .listener(errorRecordWriteListener)
                .start(processDataMesgFileStep)
                .build();

        return job;
    }

    /**
     * Step for processing data messages.
     * @param jobRepository
     * @param reader
     * @param processor
     * @param writer
     * @param recordSkipListener
     * @param taskExecutor
     * @param transactionManager
     * @return
     */
    @SuppressWarnings("unchecked")
    @Bean
    @Qualifier("processDataMesgFileStep")
    public Step processDataMesgStep(JobRepository jobRepository,
            @Qualifier("itemReader") ListItemReader reader,
            @Qualifier("coreDataProcessor") ItemProcessor processor,
            @Qualifier("dataMesgItemWriter") ItemWriter<List<KeyValue>> writer,
            @Qualifier("recordSkipListener") RecordSkipListener recordSkipListener,
            TaskExecutor taskExecutor,
            PlatformTransactionManager transactionManager) {

        Step step = new StepBuilder("processDataMesgFileStep", jobRepository)
                .chunk(chunkSize,transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .listener(recordSkipListener)
                .skip(SQLException.class)
                .skip(ExecutionException.class)
                .skip(ClassCastException.class)
                .taskExecutor(taskExecutor)
                .build();

        return step;
    }

    /**
     * Listener at record-skip.
     * @param skippedRecordsQueue
     * @return
     */
    @Bean
    @Qualifier("recordSkipListener")
    public RecordSkipListener recordSkipListener(@Qualifier("skippedRecordsQueue") ConcurrentLinkedQueue<String> skippedRecordsQueue) {
        return new RecordSkipListener(skippedRecordsQueue);
    }

    /**
     * Listener to be called after the data message process job. Logs the total counts of records read
     * and sent. Also logs the processing time taken.
     * @return
     */
    @Bean
    @Qualifier("jobLoggingListener")
    public CompositeJobExecutionListener jobLoggingListener() {
        return new JobLoggingCountsListener();
    }

    /**
     * Listener at write-errors causing skipped records.
     * @param skippedRecordsQueue
     * @return
     */
    @Bean
    @Qualifier("errorRecordWriteListener")
    public CompositeJobExecutionListener errorRecordWriteListener(@Qualifier("skippedRecordsQueue") ConcurrentLinkedQueue<String> skippedRecordsQueue) {
        return new ErrorRecordsWriteListener(skippedRecordsQueue);
    }

    /**
     * Job parameters incrementer.
     * @return
     */
    @Bean
    public JobParametersIncrementer dateIncrementor() {
        // this adds the current timestamp to the job parameters
        // spring batch has a check on start up to only allow a job to run once with a given set of parameters
        // this allows the job to be rerun
        class TestIncrementer implements JobParametersIncrementer {
            @Override
            public JobParameters getNext(JobParameters params) {
                params = (params == null) ? new JobParameters() : params;
                return new JobParametersBuilder(params).addString(PARAM_DATE, LocalDateTime.now().toString()).toJobParameters();
            }
        }

        return new TestIncrementer();
    }

    /**
     * Items reader.
     * @param message
     * @return
     */
    @Bean("itemReader")
    @StepScope
    @Qualifier("itemReader")
    @SuppressWarnings("unchecked")
    public ListItemReader reader(List message) {
        return new ListItemReader(message);
    }

    /**
     * Registers job repository.
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws BatchConfigurationException
     */
    @Bean
    public JobRepository jobRepository(DataSource  dataSource,PlatformTransactionManager transactionManager) throws BatchConfigurationException {
        JobRepository jobRepository = null;
        try {
            createJobRepository(dataSource ,transactionManager);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return jobRepository;
    }

    /**
     * Task executor.
     * @return
     */
    @Bean
    public TaskExecutor taskExecutor() {
        // messages sent via Kafka to DMP are not assumed to be ordered
        return new SimpleAsyncTaskExecutor("processDataMesgToExecutor");
    }

    /**
     * Registers {@link JobLauncher} bean.
     */
    @Bean
    public JobLauncher jobLauncher(TaskExecutor taskExecutor, JobRepository jobRepository) throws BatchConfigurationException {
        JobLauncher jobLauncher= null;
        try {
            jobLauncher = createJobLauncher(taskExecutor, jobRepository);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return jobLauncher;
    }

    @Bean
    public JobOperator jobOperator(JobLauncher jobLauncher, JobRegistry jobRegistry, JobExplorer jobExplorer,
                                   JobRepository jobRepository) {
        SimpleJobOperator jobOperator = new SimpleJobOperator();
        jobOperator.setJobExplorer(jobExplorer);
        jobOperator.setJobLauncher(jobLauncher);
        jobOperator.setJobRegistry(jobRegistry);
        jobOperator.setJobRepository(jobRepository);
        return jobOperator;
    }

    private TaskExecutorJobLauncher createJobLauncher(TaskExecutor taskExecutor, JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    private JobRepository createJobRepository(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception {
        JobRepositoryFactoryBean factoryBean = new JobRepositoryFactoryBean();
        factoryBean.setDatabaseType("ORACLE");
        factoryBean.setDataSource(dataSource);
        factoryBean.setTransactionManager(transactionManager);
        factoryBean.setIsolationLevelForCreate(isolationLevel);
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }

}
