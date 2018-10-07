package com.aashish.rescue.batch.service;

import org.springframework.stereotype.Service;

import com.aashish.rescue.batch.entity.BatchReport;

@Service("batchReportService")
public interface BatchReportService {

	void save(BatchReport report);
}
