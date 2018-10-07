package com.aashish.rescue.batch.service.impl;

import org.springframework.stereotype.Component;

import com.aashish.rescue.batch.dao.BatchReportDao;
import com.aashish.rescue.batch.entity.BatchReport;
import com.aashish.rescue.batch.service.BatchReportService;

@Component
public class BatchReportServiceImpl implements BatchReportService {

	private BatchReportDao batchReportDao;
	@Override
	public void save(BatchReport report) {
		batchReportDao.save(report);
	}

}
