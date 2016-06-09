package org.logger.event.datasource.infra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public final class CassandraClient implements Register {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
	private static Session analyticsCassandraSession;
	private static String analyticsKeyspaceName;
	private static Session eventCassandraSession;
	private static String eventKeyspaceName;
	private static KafkaLogProducer kafkaLogWriter;
	private static Properties configConstants;

	@Override
	public void init() {
		loadConfigFile();
		final String analyticsCassandraHosts = getProperty("analytics.cassandra.seeds");
		analyticsKeyspaceName = getProperty("analytics.cassandra.keyspace");
		String analyticsCassCluster = getProperty("analytics.cassandra.cluster");
		final String analyticsCassandraDatacenter = getProperty("analytics.cassandra.datacenter");

		final String eventCassandraHosts = getProperty("event.cassandra.seeds");
		eventKeyspaceName = getProperty("event.cassandra.keyspace");
		String eventCassCluster = getProperty("event.cassandra.cluster");
		final String eventCassandraDatacenter = getProperty("event.cassandra.datacenter");

		final String kafkaProducerIp = getProperty("kafka.producer.ip");
		final String kafkaLogwritterTopic = getProperty("kafka.logwritter.topic");
		final String kafkaProducerPort = getProperty("kafka.producer.port");
		final String kafkaProducerType = getProperty("kafka.producer.type");

		LOG.info("Loading cassandra properties");
		LOG.info("CASSANDRA_KEYSPACE" + analyticsKeyspaceName);
		LOG.info("CASSANDRA_IP" + analyticsCassandraHosts);
		LOG.info("DATACENTER" + analyticsCassandraDatacenter);
		LOG.info("KAFKA_LOG_WRITTER_PRODUCER_IP" + kafkaProducerIp);
		LOG.info("KAFKA_LOG_WRITTER_PRODUCER_IP" + kafkaProducerIp);

		try {
			Cluster analyticsCassandraCluster =
				Cluster.builder().withClusterName(analyticsCassCluster).addContactPoint(analyticsCassandraHosts)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
			analyticsCassandraSession = analyticsCassandraCluster.connect(analyticsKeyspaceName);

			Cluster eventCassandraCluster =
				Cluster.builder().withClusterName(eventCassCluster).addContactPoint(eventCassandraHosts)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
			eventCassandraSession = eventCassandraCluster.connect(eventKeyspaceName);

			kafkaLogWriter = new KafkaLogProducer(kafkaProducerIp, kafkaProducerPort, kafkaLogwritterTopic, kafkaProducerType);
		} catch (Exception e) {
			LOG.error("Error while initializing cassandra : {}", e);
		}
	}

	public static String getAnalyticsKeyspace() {
		return analyticsKeyspaceName;
	}

	public static String getEventKeyspace() {
		return eventKeyspaceName;
	}

	public static KafkaLogProducer getKafkaLogProducer() {
		return kafkaLogWriter;
	}

	public static Session getAnalyticsCassandraSession() {
		if (analyticsCassandraSession == null) {
			try {
				throw new IOException("Analytics Session is not initialized.");
			} catch (IOException e) {
				LOG.error("Analytics Session is not initialized.");
			}
		}
		return analyticsCassandraSession;
	}

	public static Session getEventCassandraSession() {
		if (eventCassandraSession == null) {
			try {
				throw new IOException("Event Session is not initialized.");
			} catch (IOException e) {
				LOG.error("Event Session is not initialized.");
			}
		}
		return eventCassandraSession;
	}

	private void loadConfigFile() {
		InputStream inputStream;
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

	private static final class CassandraClientHolder {
		public static final CassandraClient INSTANCE = new CassandraClient();
	}

	public static CassandraClient instance() {
		return CassandraClientHolder.INSTANCE;
	}

	public static String getProperty(String key) {
		return configConstants.getProperty(key);
	}
}
