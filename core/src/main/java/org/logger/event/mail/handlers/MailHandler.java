package org.logger.event.mail.handlers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.kafka.event.microaggregator.core.Constants;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;

public class MailHandler implements Constants {

	private static Session mailSession;

	private static BaseCassandraRepo baseCassandraRepo;

	private static Map<String, String> kafkaNotificationConfig;

	private static Logger logger = LoggerFactory.getLogger(MailHandler.class);

	public MailHandler() {
		setKafkaConfiguration();
	}

	private void setProperty(final String userName, final String password) {
		Properties props = new Properties();
		props.put("mail.transport.protocol", "smtp");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.socketFactory.port", "465");
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.port", "465");
		mailSession = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(userName, password);
			}
		});
	}

	private void setKafkaConfiguration() {
		kafkaNotificationConfig = new HashMap<String, String>();
		ColumnList<String> columnList = baseCassandraRepo.readWithKey(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), KAFKA_MAIL_HANDLER);
		kafkaNotificationConfig.put("userName", columnList.getStringValue("user_name", null));
		kafkaNotificationConfig.put("password", columnList.getStringValue("password", null));
		kafkaNotificationConfig.put("toAdress", columnList.getStringValue("to_address", null));
		kafkaNotificationConfig.put("ccAddress", columnList.getStringValue("cc_address", null));
		kafkaNotificationConfig.put("bccAddress", columnList.getStringValue("bcc_address", null));
		setProperty(columnList.getStringValue("user_name", null), columnList.getStringValue("password", null));
	}

	/**
	 * This will send the kafka status notification using configuration
	 * 
	 * @param message
	 *            The external message needs to be used in mail body.
	 */
	public void sendKafkaNotification(String message) {
		try {
			sendMail(kafkaNotificationConfig.get("userName"), kafkaNotificationConfig.get("password"), kafkaNotificationConfig.get("toAdress"), kafkaNotificationConfig.get("ccAddress"),
					kafkaNotificationConfig.get("bccAddress"), "Kafka Notification", message);
		} catch (Exception e) {
			logger.error("unable to send kafka notification");
		}
	}

	private void sendMail(String userName, String password, String to, String cc, String bcc, String subject, String text) throws MessagingException {

		Transport transport = mailSession.getTransport();
		InternetAddress[] toRecipients = InternetAddress.parse(to);
		MimeMessage message = new MimeMessage(mailSession);
		message.setRecipients(Message.RecipientType.TO, toRecipients);
		if (cc != null) {
			InternetAddress[] ccRecipients = InternetAddress.parse(cc);
			message.setRecipients(Message.RecipientType.CC, ccRecipients);
		}
		if (bcc != null) {
			InternetAddress[] bccRecipients = InternetAddress.parse(bcc);
			message.setRecipients(Message.RecipientType.BCC, bccRecipients);
		}
		message.setSubject(subject);
		message.setText(text);
		transport.connect();
		Transport.send(message);
		transport.close();
	}
}
