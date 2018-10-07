package com.aashish.rescue.batch.writer;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.aashish.rescue.batch.dao.BatchReportDao;
import com.aashish.rescue.batch.entity.BatchReport;
import com.aashish.rescue.batch.util.EmailClient;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class StockPriceAggregator.
 * 
 * 
 */
@Component
@Slf4j
public class DatabaseReportWriter implements ItemWriter<BatchReport> {

	@Autowired
	private BatchReportDao batchReportDao;
	
	@Value("${email.account}")
	private String emailAccount;

	@Value("${email.account.password}")
	private String emailPassword;

	@Value("${smtpHostServer}")
	private String smtpHostServer;

	@Value("${smtp.port}")
	private String smtpPort;

	EmailClient emailClient = new EmailClient();
	private static final Logger log = LoggerFactory.getLogger(DatabaseReportWriter.class);

	@Override
	public void write(List<? extends BatchReport> report) throws Exception {
		for(int i=0;i<report.size();i++){
			BatchReport br = report.get(i);
			sendEmailNotification(br);
			//sendSms(t);
			batchReportDao.save(br);
			log.info("Batch Writer::" + br);
		}
	}

	private void sendSms(BatchReport t) {
		// TODO Auto-generated method stub
		
	}

	private void sendEmailNotification(BatchReport report) {
		
		String subject = "Rescue batch execution status";
		/*String body = "Batch Status:" + report.getStatus() + "\n Message:" + report.getMessage() + "\n File Name:"
				+ report.getDbScriptFilePath() + "\\" + report.getDbScriptFileName();*/
		String body = report.toString();
		if(!(StringUtils.isEmpty(emailAccount) && StringUtils.isEmpty(emailPassword))){
			Boolean result = emailClient.sendEmail(emailAccount,emailPassword, report.getEmailIds(), subject, body,smtpHostServer,smtpPort);
			if(result){
				report.setEmailSentTimestamp(new Date().toString());
				report.setEmailStatus("Success");
			}else{
				report.setEmailStatus("Failed");
			}
	}
}
}
