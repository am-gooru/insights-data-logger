package org.logger.event.datasource.infra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.kafka.log.writer.producer.KafkaLogProducer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public final class CassandraClient implements Register {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
	private static Cluster cluster;
    private static Session session;
    private static String logKeyspaceName;
	private static KafkaLogProducer kafkaLogWriter;
	private static Properties configConstants;
	
	@Override
	public void init() {
		loadConfigFile();
		final String cassandraIp = getProperty("cassandra.seeds");
		logKeyspaceName = getProperty("cassandra.keyspace");
		String cassCluster = getProperty("cassandra.cluster");
		final String dataCenter = getProperty("cassandra.datacenter");
		final String kafkaProducerIp = getProperty("kafka.producer.ip");
		final String kafkaLogwritterTopic = getProperty("kafka.logwritter.topic");
		final String kafkaProducerPort = getProperty("kafka.producer.port");
		final String kafkaProducerType = getProperty("kafka.producer.type");
		
		if (cassCluster == null) {
			cassCluster = "gooru-cassandra";
		}
		LOG.info("Loading cassandra properties");
		LOG.info("CASSANDRA_KEYSPACE" + logKeyspaceName);
		LOG.info("CASSANDRA_IP" + cassandraIp);
		LOG.info("DATACENTER" + dataCenter);

		LOG.info("KAFKA_LOG_WRITTER_PRODUCER_IP" + kafkaProducerIp);
		LOG.info("KAFKA_LOG_WRITTER_PRODUCER_IP" + kafkaProducerIp);
		
		try {
			cluster = Cluster.builder().withClusterName(cassCluster).addContactPoint(cassandraIp).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
			/*
			 * .withReconnectionPolicy( new ExponentialReconnectionPolicy(1000, 30000))
			 */
			.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenter))).build();
			session = cluster.connect(logKeyspaceName);
			
			kafkaLogWriter = new KafkaLogProducer(kafkaProducerIp, kafkaProducerPort, kafkaLogwritterTopic, kafkaProducerType);
		} catch (Exception e) {
			LOG.error("Error while initializing cassandra : {}", e);
		}
	}
	public static String getLogKeyspaceName() {
		return logKeyspaceName;
	}
	public static KafkaLogProducer getKafkaLogProducer() {
		return kafkaLogWriter;
	}
	
	public static Session getCassSession() {
		if(session == null) {
			try {
				throw new IOException("Session is not initialized.");
			} catch (IOException e) {
				LOG.error("Session is not initialized.");
			}
		}
		return session;
	}  
	
	private void loadConfigFile() {
		InputStream inputStream = null;
		try {
			configConstants = new Properties();
			String propFileName = "logapi-config.properties";
			String configPath = System.getenv("CATALINA_HOME").concat("/conf/");
			inputStream = FileUtils.openInputStream(new File(configPath.concat(propFileName)));
			configConstants.load(inputStream);
			inputStream.close();
		} catch (IOException ioException) {
			LOG.error("Unable to Load Config File", ioException);
		}
	}
	
	private static class CassandraClientHolder {
		public static final CassandraClient INSTANCE = new CassandraClient();
	}

	public static CassandraClient instance() {
		return CassandraClientHolder.INSTANCE;
	}
    
	
	public static String getProperty(String key) {
		return configConstants.getProperty(key);
	}
}
