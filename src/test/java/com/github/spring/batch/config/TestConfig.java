package com.github.spring.batch.config;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.commons.collections.KeyValue;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.scope.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.adapter.ItemProcessorAdapter;
import org.springframework.beans.factory.annotation.Qualifier; 
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.embedded.ConnectionProperties;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseConfigurer;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseFactory;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.test.util.ReflectionTestUtils;
import com.github.spring.batch.writer.DataMessageWriter;
import org.springframework.util.ClassUtils;


/**
 * Test config overriding beans of batch-job-config.
 */
@Configuration
@EnableBatchProcessing
public class TestConfig  {

	private static final Logger log = LoggerFactory.getLogger(TestConfig.class);

	@Bean(name="dataMesgParamsValidator")
	public JobParametersValidator mockValidator() throws JobParametersInvalidException {
		DefaultJobParametersValidator validator = Mockito.mock(DefaultJobParametersValidator.class);
		Mockito.doCallRealMethod().when(validator).validate(Mockito.any());
		return validator;
	}

	@Bean(name = "coreDataProcessor")
	public ItemProcessor<?, ?> mockItemProcessor() {
		log.info("using mocked item processor");
		return Mockito.mock(ItemProcessorAdapter.class);
	}

	@Bean(name = "dataMesgItemWriter")
	public DataMessageWriter mockItemWriter()
			throws InterruptedException, ExecutionException, IOException {
		log.info("using mocked item processor");
		
		class TestDataMessageWriter extends DataMessageWriter {
			
			@Override
			protected void send(byte[] mesg) throws InterruptedException, ExecutionException, IOException {
				super.send(mesg);
			}

			@Override
			public void write(Chunk<? extends List<KeyValue>> chunk) throws IOException {

			}
		}
		
		TestDataMessageWriter mockItemWriter = Mockito.mock(TestDataMessageWriter.class);
		Mockito.doCallRealMethod().when(mockItemWriter).write( ArgumentMatchers.<List<? extends List<KeyValue>>>any() );
		Mockito.doCallRealMethod().when(mockItemWriter).send(Mockito.any());
		ReflectionTestUtils.setField(mockItemWriter, "log", Mockito.mock(Logger.class));
				
		return mockItemWriter;
	}

	@Bean
	@Qualifier("skippedRecordsQueue")
	public ConcurrentLinkedQueue<String> skippedRecordsQueue() {
		return new ConcurrentLinkedQueue<String>();
	}

	@Bean(name = "dataSource")
	public DataSource dataSource() {
		ResourceLoader defaultResourceLoader = new DefaultResourceLoader();
		EmbeddedDatabaseFactory embeddedDatabaseFactory = new EmbeddedDatabaseFactory();
		EmbeddedDatabaseConfigurer embeddedDatabaseConfigurer = new EmbeddedDatabaseConfigurer() {

			@Override
			@SuppressWarnings("unchecked")
			public void configureConnectionProperties(ConnectionProperties properties, String databaseName) {
				try {
					properties.setDriverClass((Class<? extends Driver>) ClassUtils.forName("org.hsqldb.jdbcDriver",
							this.getClass().getClassLoader()));
				}
				catch (Exception e) {
				}
				properties.setUrl("jdbc:hsqldb:mem:testdb;sql.enforce_strict_size=true;hsqldb.tx=mvcc");
				properties.setUsername("sa");
				properties.setPassword("");
			}

			@Override
			public void shutdown(DataSource dataSource, String databaseName) {
				try {
					Connection connection = dataSource.getConnection();
					Statement stmt = connection.createStatement();
					stmt.execute("SHUTDOWN");
				}
				catch (SQLException ex) {
				}
			}
		};
		embeddedDatabaseFactory.setDatabaseConfigurer(embeddedDatabaseConfigurer);

		ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
		databasePopulator.addScript(defaultResourceLoader
				.getResource("classpath:org/springframework/batch/core/schema-drop-hsqldb.sql"));
		databasePopulator.addScript(
				defaultResourceLoader.getResource("classpath:org/springframework/batch/core/schema-hsqldb.sql"));
		embeddedDatabaseFactory.setDatabasePopulator(databasePopulator);
		embeddedDatabaseFactory.setGenerateUniqueDatabaseName(true);

		return embeddedDatabaseFactory.getDatabase();
	}

	@Bean(name = "transactionManager")
	public JdbcTransactionManager transactionManager(DataSource dataSource) {
		return new JdbcTransactionManager(dataSource);
	}

	@Bean
	@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE,
			proxyMode = ScopedProxyMode.TARGET_CLASS)
	public static StepScope createStepScope() {
		StepScope stepScope = new StepScope();
		stepScope.setAutoProxy(true);
		return stepScope;
	}

	@Bean
	@Primary
	public JobRepository createJobRepository(DataSource dataSource, JdbcTransactionManager transactionManager) throws Exception {
		log.warn(">>> USING IN-MEMORY jobRepository. Should only be used for testing.  <<<");
		JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
		factory.setDatabaseType("HSQL"); // In-memory DB only for testing.
		factory.setDataSource(dataSource);
		factory.setTransactionManager(transactionManager);
		factory.afterPropertiesSet();
		JobRepository jobRepository = factory.getObject();
		return jobRepository;
	}

}
