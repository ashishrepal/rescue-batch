package com.aashish.rescue.batch.util;

import java.util.Date;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmailClient {

	public Boolean sendEmail(String emailAccount, String emailPassword, String toEmail, String subject, String body, String smtpServer, String smtpPort){
		try
	    {
			Properties props = System.getProperties();
			props.put("mail.smtp.host", smtpServer);
			props.put("mail.smtp.starttls.enable","true");
			props.put("mail.smtp.port", smtpPort); //TLS Port
	        props.put("mail.smtp.auth", "true"); //enable authentication
	        //create Authenticator object to pass in Session.getInstance argument
	        Authenticator auth = new Authenticator() {
	            //override the getPasswordAuthentication method
	            protected PasswordAuthentication getPasswordAuthentication() {
	                return new PasswordAuthentication(emailAccount, emailPassword);
	            }
	        };
	        Session session = Session.getInstance(props, auth);

	      MimeMessage msg = new MimeMessage(session);
	      //set message headers
	      msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
	      msg.addHeader("format", "flowed");
	      msg.addHeader("Content-Transfer-Encoding", "8bit");

	      msg.setFrom(new InternetAddress(emailAccount, "Rescue-Bacth"));

	      msg.setReplyTo(InternetAddress.parse(emailAccount, false));

	      msg.setSubject(subject, "UTF-8");

	      msg.setText(body, "UTF-8");

	      msg.setSentDate(new Date());

	      msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
	      System.out.println("Message is ready");
    	  Transport.send(msg);  

	      System.out.println("EMail Sent Successfully!!");
	      
	      log.info("EMail Sent Successfully To:"+toEmail + "Body:"+body);
	      return true;
	    }
	    catch (Exception e) {
	     log.error("Error occurred while sending email notification",e);
	     return false;
	    }
	}

}
