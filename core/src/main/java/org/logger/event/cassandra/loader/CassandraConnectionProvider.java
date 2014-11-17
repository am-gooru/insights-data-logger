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
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Repository
public class CassandraConnectionProvider {

    private Properties properties;
    private Keyspace cassandraKeyspace;
    private Keyspace cassandraAwsKeyspace;
    private Keyspace cassandraNewAwsKeyspace;
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);
    private static String CASSANDRA_IP;
    private static String CASSANDRA_PORT;
    private static String CASSANDRA_KEYSPACE;
    private Client devClient;
    private Client prodClient;
    private static String AWS_CASSANDRA_KEYSPACE;
    private static String AWS_CASSANDRA_IP;
    private static String INSIHGHTS_DEV_ES_IP;
    private static String INSIHGHTS_PROD_ES_IP;
    protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
    
    public void init(Map<String, String> configOptionsMap) {

        properties = new Properties();
        CASSANDRA_IP = System.getenv("INSIGHTS_CASSANDRA_IP");
        CASSANDRA_PORT = System.getenv("INSIGHTS_CASSANDRA_PORT");
        CASSANDRA_KEYSPACE = System.getenv("INSIGHTS_CASSANDRA_KEYSPACE");
        INSIHGHTS_DEV_ES_IP   = System.getenv("INSIHGHTS_DEV_ES_IP");
        INSIHGHTS_PROD_ES_IP   = System.getenv("INSIHGHTS_PROD_ES_IP");
        AWS_CASSANDRA_IP = System.getenv("AWS_CASSANDRA_IP");
        AWS_CASSANDRA_KEYSPACE = System.getenv("AWS_CASSANDRA_KEYSPACE");
        
        String esClusterName = "";
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

            properties.load(CassandraDataLoader.class
                    .getResourceAsStream("/loader.properties"));
            String clusterName = properties.getProperty("cluster.name",
                    "gooruinsights");
            if(cassandraKeyspace == null ){
            ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
                    .setPort(9160)
                    .setMaxConnsPerHost(10)
                    .setSeeds(hosts)
                    .setInitConnsPerHost(1)
                    ;
            
            if (!hosts.startsWith("127.0")) {
                poolConfig.setLocalDatacenter("datacenter1");
            }

            //poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range

            AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                    .forCluster(clusterName)
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
            if(cassandraAwsKeyspace == null ){
            	cassandraAwsKeyspace = this.initializeAwsCassandra();
            }
            if(cassandraNewAwsKeyspace == null ){
            	cassandraNewAwsKeyspace = this.initializeNewAwsCassandra();
            }

            //Elastic search connection provider
            if(devClient == null) {
	           Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", esClusterName).put("client.transport.sniff", true).build();
	           TransportClient transportClient = new TransportClient(settings);
	           transportClient.addTransportAddress(new InetSocketTransportAddress(INSIHGHTS_DEV_ES_IP, esPort));
	           
	           devClient = transportClient;
            }
            
            if(prodClient == null) {
 	           Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", esClusterName).put("client.transport.sniff", true).build();
 	           TransportClient transportClient = new TransportClient(settings);
 	           transportClient.addTransportAddress(new InetSocketTransportAddress(INSIHGHTS_PROD_ES_IP, esPort));
 	           
 	           prodClient = transportClient;
             }
            
          this.registerDevIndices();
           this.registerProdIndices();
        } catch (IOException e) {
            logger.info("Error while initializing cassandra", e);
        }
    }

	public  Keyspace initializeAwsCassandra(){
	 
		String awsHosts =  AWS_CASSANDRA_IP;
		String awsCluster = "gooru-cassandra";
		String keyspace = AWS_CASSANDRA_KEYSPACE;
		ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
	    .setPort(9160)
	    .setMaxConnsPerHost(3)
	    .setSeeds(awsHosts);
		
		if (!awsHosts.startsWith("127.0")) {
			poolConfig.setLocalDatacenter("us-west");
		}
	
		//poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range
	
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
		    .forCluster(awsCluster)
		    .forKeyspace(keyspace)
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
		    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		    .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
		    .withConnectionPoolConfiguration(poolConfig)
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());
	
		context.start();
		
		cassandraAwsKeyspace = (Keyspace) context.getClient();
		
        logger.info("Initialized connection to AWS Cassandra");
        
		return cassandraAwsKeyspace;
        
	}    

	public  Keyspace initializeNewAwsCassandra(){
		 
		String awsNewHosts =  "54.193.233.64";
		String awsNewCluster = "gooru-cassandra-prod";
		String keyspace = CASSANDRA_KEYSPACE;
		ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
	    .setPort(9160)
	    .setMaxConnsPerHost(3)
	    .setSeeds(awsNewHosts);
		
		if (!awsNewHosts.startsWith("127.0")) {
			poolConfig.setLocalDatacenter("us-west");
		}
	
		//poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl()); // Enabled SMA.  Omit this to use round robin with a token range
	
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
		    .forCluster(awsNewCluster)
		    .forKeyspace(keyspace)
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
		    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		    .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
		    .withConnectionPoolConfiguration(poolConfig)
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());
	
		context.start();
		
		cassandraNewAwsKeyspace = (Keyspace) context.getClient();
		
        logger.info("Initialized connection to New AWS Cassandra");
        
		return cassandraNewAwsKeyspace;
        
	}
	
	public Keyspace getKeyspace() throws IOException {
        if (cassandraKeyspace == null) {
            throw new IOException("Keyspace not initialized.");
        }
        return cassandraKeyspace;
    }
    public Keyspace getAwsKeyspace() throws IOException {
        if (cassandraAwsKeyspace == null) {
            throw new IOException("Keyspace not initialized.");
        }
        return cassandraAwsKeyspace;
    }
    
    public Keyspace getNewAwsKeyspace() throws IOException {
        if (cassandraNewAwsKeyspace == null) {
            throw new IOException("New Keyspace not initialized.");
        }
        return cassandraNewAwsKeyspace;
    }
    
    public Client getDevESClient() throws IOException{
    	if (devClient == null) {
            throw new IOException("Dev Elastic Search is not initialized.");
        }
    	return devClient;
    }
    public Client getProdESClient() throws IOException{
    	if (prodClient == null) {
            throw new IOException("Prod Elastic Search is not initialized.");
        }
    	return prodClient;
    }
    
    public final void registerDevIndices() {
    	String indexingVersion = readWithKey(cassandraKeyspace, org.logger.event.cassandra.loader.ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "index_version").getStringValue("constant_value", "v1");
    	if(indexingVersion != null){
	    	for (ESIndexices esIndex : ESIndexices.values()) {
				String indexName = esIndex.getIndex()+"_"+indexingVersion;
				for (String indexType : esIndex.getType()) {
					String mapping = EsMappingUtil.getMappingConfig(indexType);
					try {
						CreateIndexRequestBuilder prepareCreate = this.getDevESClient().admin().indices().prepareCreate(indexName);
						prepareCreate.addMapping(indexType, mapping);
						prepareCreate.execute().actionGet();
						logger.info("Dev Index created : " + indexName + "\n");
					} catch (Exception exception) {
						logger.info("Already Index availble : " + indexName + "\n");
					}
				
				
				}
			}
    	}
	}
    public final void registerProdIndices() {
    	String indexingVersion = readWithKey(cassandraKeyspace, org.logger.event.cassandra.loader.ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "index_version").getStringValue("constant_value", "v1");
    	if(indexingVersion != null){
    	for (ESIndexices esIndex : ESIndexices.values()) {
			String indexName = esIndex.getIndex()+"_"+indexingVersion;
			for (String indexType : esIndex.getType()) {
				String mapping = EsMappingUtil.getMappingConfig(indexType);
				try {
					CreateIndexRequestBuilder prepareProdCreate = this.getProdESClient().admin().indices().prepareCreate(indexName);
					prepareProdCreate.addMapping(indexType, mapping);
					prepareProdCreate.execute().actionGet();
					logger.info("Prod Index created : " + indexName + "\n");
					
				} catch (Exception exception) {
					logger.info("Already Index availble : " + indexName + "\n");
				}
			}
		}
    }
}
    
    public ColumnList<String> readWithKey(Keyspace keyspace,String cfName,String key){
        
    	ColumnList<String> result = null;
    	try {
              result = keyspace.prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKey(key)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
        	e.printStackTrace();
        }
    	
    	return result;
    }
    
    public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return aggregateColumnFamily;
	}
}
