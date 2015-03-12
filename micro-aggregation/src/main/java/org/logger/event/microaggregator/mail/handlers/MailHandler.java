package org.logger.event.microaggregator.mail.handlers;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.Constants;
import org.kafka.event.microaggregator.dao.AggregationDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;

public class MailHandler implements Constants {

	private static Session mailSession;
	
	private static AggregationDAOImpl baseCassandraRepoImpl;
	
	private static Logger logger = LoggerFactory.getLogger(MailHandler.class);

	public MailHandler(CassandraConnectionProvider cassandraConnectionProvider){
		baseCassandraRepoImpl = new AggregationDAOImpl(cassandraConnectionProvider);		
	}
	
	private static void setProperty(final String userName, final String password){
		try{
		Properties props = new Properties();
		props.put("mail.transport.protocol", "smtp");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.socketFactory.port", "465");
		props.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.port", "465");
		mailSession = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
		protected PasswordAuthentication getPasswordAuthentication() {
			return new PasswordAuthentication(userName,password);
		}
	});	
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * This will send the kafka status notification using configuration
	 * @param message The external message needs to be used in mail body.
	 */
	public void sendKafkaNotification(String message){
		try{
		ColumnList<String> columnList = baseCassandraRepoImpl.readRow(columnFamily.JOB_CONFIG_SETTING.columnFamily(), KAFKA_MAIL_HANDLER, null).getResult();
		String userName = columnList.getStringValue("user_name", null);
		String password = columnList.getStringValue("password", null);
		String toAddress = columnList.getStringValue("to_address", null);
		String ccAddress = columnList.getStringValue("cc_address", null);
		String bccAddress = columnList.getStringValue("bcc_address", null);
		sendMail(userName, password, toAddress, ccAddress, bccAddress,"Kafka Notification",message);
		}catch(Exception e){
			logger.error("unable to send kafka notification");
		}
	}
	
	private static void sendMail(String userName,String password,String to,String cc,String bcc,String subject,String text) throws MessagingException {
	
		setProperty(userName, password);
		Transport transport = mailSession.getTransport();
		InternetAddress[] toRecipients = InternetAddress.parse(to);
		MimeMessage message = new MimeMessage(mailSession);
		message.setRecipients(Message.RecipientType.TO,
				toRecipients);
		if(cc != null){
			InternetAddress[] ccRecipients = InternetAddress.parse(cc);
			message.setRecipients(Message.RecipientType.CC,
					ccRecipients);
		}
		if(bcc != null){
			InternetAddress[] bccRecipients = InternetAddress.parse(bcc);
			message.setRecipients(Message.RecipientType.BCC,
					bccRecipients);
		}
		message.setSubject(subject);
		message.setText(text);
		transport.connect();
		Transport.send(message);
		transport.close();
	}
}
