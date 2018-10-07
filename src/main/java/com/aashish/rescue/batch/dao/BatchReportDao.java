package com.aashish.rescue.batch.dao;

import javax.transaction.Transactional;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Component;

import com.aashish.rescue.batch.entity.BatchReport;


@Transactional
@Component
public interface BatchReportDao extends CrudRepository<BatchReport, Long>{

	/**
	   * This method will find an User instance in the database by its email.
	   * Note that this method is not implemented and its working code will be
	   * automagically generated from its signature by Spring Data JPA.
	   */
	  public BatchReport findById(String id);

}
