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
import java.net.InetAddress;
import java.util.Map;

import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.UserCo;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public final class CassandraConnectionProvider {

    private static Keyspace cassandraKeyspace;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectionProvider.class);
    private static String cassandraIp;
    private static String cassKeyspace;
    private Client client;
    private static String cassCluster;
    private static String elsIp;
    private static String elsCluster;
    private static String dataCenter;
	private EntityManager<ResourceCo, String> resourceEntityPersister;
	private EntityManager<UserCo, String> userEntityPersister;
    
    public final void init(Map<String, String> configOptionsMap) {
        cassandraIp = System.getenv("INSIGHTS_CASSANDRA_IP");
        cassKeyspace = System.getenv("INSIGHTS_CASSANDRA_KEYSPACE");
        elsIp   = System.getenv("INSIGHTS_ES_IP");
        cassCluster = System.getenv("CASSANDRA_CLUSTER");
        elsCluster = System.getenv("ES_CLUSTER");
        dataCenter = System.getenv("DATACENTER");
       
        if(cassCluster == null){
        	cassCluster = "gooru-cassandra";
        }
        
        if(elsCluster == null){
        	elsCluster = "gooru-es";
        }

        try {
            LOG.info("Loading cassandra properties"); 
            String hosts = cassandraIp;
            String keyspace = cassKeyspace;
            
            LOG.info("CASSANDRA_CLUSTER" + cassCluster);
            LOG.info("CASSANDRA_KEYSPACE" + keyspace);
            LOG.info("CASSANDRA_IP" + hosts);
            LOG.info("DATACENTER" + dataCenter);
            
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
                poolConfig.setLocalDatacenter(dataCenter);
            }

            AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                    .forCluster(cassCluster)
                    .forKeyspace(keyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                    .setCqlVersion("3.0.0")
                    .setTargetCassandraVersion("2.1.4")
                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                    .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
                    .withConnectionPoolConfiguration(poolConfig)
                    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            context.start();

            cassandraKeyspace = (Keyspace) context.getClient();
            LOG.info("Initialized connection to Cassandra");
            }

            if(cassandraKeyspace != null ) {
				resourceEntityPersister = new DefaultEntityManager.Builder<ResourceCo, String>().withEntityType(ResourceCo.class).withKeyspace(getKeyspace()).build();
				userEntityPersister = new DefaultEntityManager.Builder<UserCo, String>().withEntityType(UserCo.class).withKeyspace(getKeyspace()).build();
			}
            
            if(client == null){
               //Elastic search connection provider
                  Settings settings = Settings.settingsBuilder().put("cluster.name", elsCluster).put("client.transport.sniff", true).build();
                  TransportClient transportClient = TransportClient.builder().settings(settings).build()
			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elsIp), 9300));
	           client = transportClient;
            }
            
          // this.registerIndices();
        } catch (IOException e) {
            LOG.info("Error while initializing cassandra", e);
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
					LOG.info("Index created : " + indexName + "\n");
				} catch (IndexAlreadyExistsException exception) {
					LOG.info(indexName +" index already exists! \n");
					//this.getESClient().admin().indices().preparePutMapping(indexName).setType(indexType).setSource(mapping).setIgnoreConflicts(true).execute().actionGet();
				} catch (Exception e) {
					LOG.info("Unable to create Index : " + indexName + "\n", e.getMessage());
				}
			}
		}
	}
}
