/*******************************************************************************
 * CassandraConnectionProvider.java
 * insights-event-logger
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.logger.event.cassandra.loader;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.UserCo;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Repository
public class CassandraConnectionProvider {

    private Properties properties;
    private static Keyspace cassandraKeyspace;
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);
    private static String CASSANDRA_IP;
    private static String CASSANDRA_KEYSPACE;
    private Client client;
    private static String CASSANDRA_CLUSTER;
    private static String INSIHGHTS_ES_IP;
    private static String ES_CLUSTER;
	private EntityManager<ResourceCo, String> resourceEntityPersister;
	private EntityManager<UserCo, String> userEntityPersister;
    
    public void init(Map<String, String> configOptionsMap) {

        properties = new Properties();
        CASSANDRA_IP = System.getenv("INSIGHTS_CASSANDRA_IP");
        CASSANDRA_KEYSPACE = System.getenv("INSIGHTS_CASSANDRA_KEYSPACE");
        INSIHGHTS_ES_IP   = System.getenv("INSIHGHTS_ES_IP");
        CASSANDRA_CLUSTER = System.getenv("CASSANDRA_CLUSTER");
        ES_CLUSTER = System.getenv("ES_CLUSTER");;
        
       
        if(CASSANDRA_CLUSTER == null){
        	CASSANDRA_CLUSTER = "gooru-cassandra";
        }
        
        if(ES_CLUSTER == null){
        	ES_CLUSTER = "gooru-es";
        }
        int esPort = 9300;
        try {

            logger.info("Loading cassandra properties");
            String hosts = CASSANDRA_IP;
            String keyspace = CASSANDRA_KEYSPACE;

            if (configOptionsMap != null) {
                hosts = StringUtils.defaultIfEmpty(
                        configOptionsMap.get("hosts"), hosts);
                keyspace = StringUtils.defaultIfEmpty(
                        configOptionsMap.get("keyspace"), keyspace);
            }
            
            logger.info("CASSANDRA_CLUSTER" + CASSANDRA_CLUSTER);
            
            properties.load(CassandraDataLoader.class
                    .getResourceAsStream("/loader.properties"));
            String clusterName = properties.getProperty("cluster.name",
                    "gooruinsights");

            if(cassandraKeyspace == null){
            ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
                    .setPort(9160)
                    .setSeeds(hosts)
                    .setSocketTimeout(30000)
					.setMaxTimeoutWhenExhausted(2000)
					.setMaxConnsPerHost(10)
					.setInitConnsPerHost(1)
					
                    ;
            
            if (!hosts.startsWith("127.0")) {
                poolConfig.setLocalDatacenter("us-west");
            }

            AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                    .forCluster(CASSANDRA_CLUSTER)
                    .forKeyspace(keyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                    .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
                    .withConnectionPoolConfiguration(poolConfig)
                    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            context.start();

            cassandraKeyspace = (Keyspace) context.getClient();
            logger.info("Initialized connection to Cassandra");
            }

            if(cassandraKeyspace != null ) {
				resourceEntityPersister = new DefaultEntityManager.Builder<ResourceCo, String>().withEntityType(ResourceCo.class).withKeyspace(getKeyspace()).build();
				userEntityPersister = new DefaultEntityManager.Builder<UserCo, String>().withEntityType(UserCo.class).withKeyspace(getKeyspace()).build();
			}
            
            if(client == null){
               //Elastic search connection provider
	           Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", ES_CLUSTER).put("client.transport.sniff", true).build();
	           TransportClient transportClient = new TransportClient(settings);
	           transportClient.addTransportAddress(new InetSocketTransportAddress(INSIHGHTS_ES_IP, esPort));
	           client = transportClient;
            }
            
           this.registerIndices();
        } catch (IOException e) {
            logger.info("Error while initializing cassandra", e);
        }
    }

	   
    public Keyspace getKeyspace() throws IOException {
        if (cassandraKeyspace == null) {
            throw new IOException("Keyspace not initialized.");
        }
        return cassandraKeyspace;
    }
    
    public EntityManager<ResourceCo, String> getResourceEntityPersister() throws IOException {
    	if (resourceEntityPersister == null) {
            throw new IOException("Resource Entity is not persisted");
        }
    	return resourceEntityPersister;
    }
    public EntityManager<UserCo, String> getUserEntityPersister() throws IOException {
    	if (userEntityPersister == null) {
            throw new IOException("User Entity is not persisted");
        }
    	return userEntityPersister;
    }
    public Client getESClient() throws IOException{
    	if (client == null) {
            throw new IOException("Elastic Search is not initialized.");
        }
    	return client;
    }
    public final void registerIndices() {
		for (ESIndexices esIndex : ESIndexices.values()) {
			String indexName = esIndex.getIndex();
			for (String indexType : esIndex.getType()) {
				//String setting = EsMappingUtil.getSettingConfig(indexType);
				String mapping = EsMappingUtil.getMappingConfig(indexType);
				try {
					CreateIndexRequestBuilder prepareCreate = this.getESClient().admin().indices().prepareCreate(indexName);
					//prepareCreate.setSettings(setting);
					prepareCreate.addMapping(indexType, mapping);
					prepareCreate.execute().actionGet();
					logger.info("Index created : " + indexName + "\n");
				} catch (Exception exception) {
					logger.info("Already Index availble : " + indexName + "\n");
					//this.getESClient().admin().indices().preparePutMapping(indexName).setType(indexType).setSource(mapping).setIgnoreConflicts(true).execute().actionGet();
				}
			}
		}
	}
}
