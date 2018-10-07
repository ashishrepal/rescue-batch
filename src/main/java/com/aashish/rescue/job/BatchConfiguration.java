package com.aashish.rescue.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

import com.aashish.rescue.batch.entity.BatchReport;
import com.aashish.rescue.batch.listener.JobCompletionNotificationListener;
import com.aashish.rescue.batch.model.InOutDatabaseDetails;
import com.aashish.rescue.batch.processor.DatabaseEventProcessor;
import com.aashish.rescue.batch.quartz.QuartzConfiguration;
import com.aashish.rescue.batch.reader.DatabaseFileReader;
import com.aashish.rescue.batch.service.BatchReportService;
import com.aashish.rescue.batch.service.impl.BatchReportServiceImpl;
import com.aashish.rescue.batch.writer.DatabaseReportWriter;

/**
 * 
 * 
 */
@Configuration
@EnableBatchProcessing
@Import({QuartzConfiguration.class})
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Value("${staging.directory}")
	String stagingDirectory;
	
	@Autowired
	private Environment env;

	@Bean
	public DatabaseFileReader databaseFileReader() throws Exception {
		return new DatabaseFileReader(stagingDirectory);
	}

	@Bean
	public DatabaseEventProcessor databaseEventProcessor() {
		return new DatabaseEventProcessor();
	}

	@Bean
	public DatabaseReportWriter databaseReportWriter() {
		return new DatabaseReportWriter();
	}

	// JobCompletionNotificationListener (File loader)
	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionNotificationListener();
	}

	//service
	
	public BatchReportService batchReportService(){
		return new BatchReportServiceImpl();
	}
	
	// Configure job step
	@Bean
	public Job rescueJob() throws Exception {
		return jobBuilderFactory.get("rescue_batch_job").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(etlStep()).end().build();
	}

	@Bean
	public Step etlStep() throws Exception {
		//databaseFileReader();
		// The job is thus scheduled to run every 2 minute. In fact it should
		// be successful on the first attempt, so the second and subsequent
		// attempts should through a JobInstanceAlreadyCompleteException, so you have to set allowStartIfComplete to true
//		return stepBuilderFactory.get("Extract -> Transform -> Aggregate -> Load").allowStartIfComplete(true)
//				.<FxMarketEvent, Trade> chunk(10000).reader(fxMarketEventReader()).processor(fxMarketEventProcessor())
//				.writer(stockPriceAggregator()).build();
		
		return stepBuilderFactory.get("Extract -> Transform -> Aggregate -> Load").allowStartIfComplete(true)
				.<InOutDatabaseDetails, BatchReport> chunk(10000).reader(databaseFileReader()).processor(databaseEventProcessor())
				.writer(databaseReportWriter())
				.build();
	}

}
