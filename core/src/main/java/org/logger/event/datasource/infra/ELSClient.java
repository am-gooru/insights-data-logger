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
