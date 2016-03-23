package org.logger.event.datasource.infra;

import java.io.IOException;
import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ELSClient implements Register {

	private static final Logger LOG = LoggerFactory.getLogger(ELSClient.class);

	private static ELSClient elsClient = null;

	private static Client client;

	@Override
	public void init() {
		final String elsIp = CassandraClient.getProperty("elasticsearch.ip");
		String elsCluster = CassandraClient.getProperty("elasticsearch.cluster");;

		LOG.info("ELS IP : " + elsIp);
		if(elsCluster == null){
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
}
