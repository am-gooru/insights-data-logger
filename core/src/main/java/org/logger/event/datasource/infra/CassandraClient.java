package org.logger.event.datasource.infra;

import java.io.IOException;

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
    
	@Override
	public void init() {

		final String cassandraIp = System.getenv("INSIGHTS_CASSANDRA_IP");
		logKeyspaceName = System.getenv("INSIGHTS_CASSANDRA_KEYSPACE");
		String cassCluster = System.getenv("CASSANDRA_CLUSTER");
		final String dataCenter = System.getenv("DATACENTER");
		if (cassCluster == null) {
			cassCluster = "gooru-cassandra";
		}
		LOG.info("Loading cassandra properties");
		LOG.info("CASSANDRA_KEYSPACE" + logKeyspaceName);
		LOG.info("CASSANDRA_IP" + cassandraIp);
		LOG.info("DATACENTER" + dataCenter);

		try {
			cluster = Cluster.builder().withClusterName(cassCluster).addContactPoint(cassandraIp).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
			/*
			 * .withReconnectionPolicy( new ExponentialReconnectionPolicy(1000, 30000))
			 */
			.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenter))).build();
			session = cluster.connect(logKeyspaceName);

		} catch (Exception e) {
			LOG.error("Error while initializing cassandra : {}", e);
		}
	}
	public static String getLogKeyspaceName() {
		return logKeyspaceName;
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
	private static class CassandraClientHolder {
		public static final CassandraClient INSTANCE = new CassandraClient();
	}

	public static CassandraClient instance() {
		return CassandraClientHolder.INSTANCE;
	}
    
}
