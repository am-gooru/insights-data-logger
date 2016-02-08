package org.logger.event.datasource.infra;

import java.io.IOException;
import java.net.InetAddress;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.logger.event.cassandra.loader.ESIndexices;
import org.logger.event.cassandra.loader.EsMappingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ELSClient implements Register {

	private static final Logger LOG = LoggerFactory.getLogger(ELSClient.class);

	private static ELSClient elsClient = null;

	private static Client client;

	@Override
	public void init() {
		String elsIp = System.getenv("INSIGHTS_ES_IP");
		String elsCluster = System.getenv("ES_CLUSTER");

		if (elsCluster == null) {
			elsCluster = "gooru-es";
		}
		if (client == null) {
			try {
				// Elastic search connection provider
				Settings settings = Settings.settingsBuilder().put("cluster.name", elsCluster).put("client.transport.sniff", true).build();
				TransportClient transportClient;
				transportClient = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elsIp), 9300));
				client = transportClient;
				//registerIndices();
			} catch (Exception e) {
				LOG.error("Exception while initializing ELS", e);
			}
		}
	}

	public static ELSClient instance() {
		if (elsClient == null) {
			synchronized (ELSClient.class) {
				elsClient = new ELSClient();
			}
		}
		return elsClient;
	}

	public static Client getESClient() throws IOException {
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
