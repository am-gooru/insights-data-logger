package org.logger.event.datasource.infra;

import java.io.IOException;

import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.UserCo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public final class CassandraClient implements Register {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
	private static Keyspace cassandraKeyspace;
	private static EntityManager<ResourceCo, String> resourceEntityPersister;
	private static EntityManager<UserCo, String> userEntityPersister;
	private static Cluster cluster;
    private static Session session;
    private static String logKeyspaceName;
    
	@Override
	public void init() {

		final String cassandraIp = System.getenv("INSIGHTS_CASSANDRA_IP");
		final String cassKeyspace = System.getenv("INSIGHTS_CASSANDRA_KEYSPACE");
		String cassCluster = System.getenv("CASSANDRA_CLUSTER");
		final String dataCenter = System.getenv("DATACENTER");
		if(cassCluster == null){
		   cassCluster = "gooru-cassandra";
		}
		try {
			LOG.info("Loading cassandra properties");
			LOG.info("CASSANDRA_KEYSPACE" + cassKeyspace);
			LOG.info("CASSANDRA_IP" + cassandraIp);
			LOG.info("DATACENTER" + dataCenter);

			if (cassandraKeyspace == null) {
				ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setSeeds(cassandraIp).setSocketTimeout(30000)
						.setMaxTimeoutWhenExhausted(2000).setMaxConnsPerHost(10).setInitConnsPerHost(1)

				;

				if (!cassandraIp.startsWith("127.0")) {
					poolConfig.setLocalDatacenter(dataCenter);
				}

				AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
						.forCluster(cassCluster)
						.forKeyspace(cassKeyspace)
						.withAstyanaxConfiguration(
								new AstyanaxConfigurationImpl().setCqlVersion("3.0.0").setTargetCassandraVersion("2.1.4").setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
										.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)).withConnectionPoolConfiguration(poolConfig)
						.withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());

				context.start();

				cassandraKeyspace = (Keyspace) context.getClient();
				LOG.info("Initialized connection to Cassandra");

				if (cassandraKeyspace != null) {
					resourceEntityPersister = new DefaultEntityManager.Builder<ResourceCo, String>().withEntityType(ResourceCo.class).withKeyspace(getKeyspace()).build();
					userEntityPersister = new DefaultEntityManager.Builder<UserCo, String>().withEntityType(UserCo.class).withKeyspace(getKeyspace()).build();
				}

			}

		} catch (Exception e) {
			LOG.error("Cassandra Exception fails", e);
		}
			try {
				cluster = Cluster
						.builder()
						.withClusterName(cassCluster)
						.addContactPoint(cassandraIp)
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						/*.withReconnectionPolicy(
								new ExponentialReconnectionPolicy(1000, 30000))*/
						.withLoadBalancingPolicy(
								new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenter)))
						.build();
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
    
	public static Keyspace getKeyspace() throws IOException {
		if (cassandraKeyspace == null) {
			throw new IOException("Keyspace not initialized.");
		}
		return cassandraKeyspace;
	}

	public static EntityManager<ResourceCo, String> getResourceEntityPersister() throws IOException {
		if (resourceEntityPersister == null) {
			throw new IOException("Resource Entity is not persisted");
		}
		return resourceEntityPersister;
	}

	public static EntityManager<UserCo, String> getUserEntityPersister() throws IOException {
		if (userEntityPersister == null) {
			throw new IOException("User Entity is not persisted");
		}
		return userEntityPersister;
	}
}
