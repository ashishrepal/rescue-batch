package com.aashish.rescue.job;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * The Class Application.
 * 
 * 
 */
@SpringBootApplication
@EnableJpaRepositories("com.aashish.rescue.batch.dao")
//@ComponentScan(basePackages = {"com.aashish.rescue.batch.util.*"})
@EntityScan("com.aashish.rescue.batch.entity") 
public class Application {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Application.class, args);
	}
}
