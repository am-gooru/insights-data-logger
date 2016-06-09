/*******************************************************************************
 * MessageConsumer.java
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
package org.ednovo.kafka.consumer;

/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.datasource.infra.CassandraClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumer extends Thread implements Runnable {

	private static ConsumerConnector consumer;
	private final DataProcessor rowDataProcessor;

	private static String[] topic;
	private static String ZK_IP;
	private static String ZK_PORT;
	private static String KAFKA_GROUPID;
	private final ExecutorService service = Executors.newFixedThreadPool(10);

	private static final Logger LOG = LoggerFactory.getLogger(MessageConsumer.class);

	public MessageConsumer(DataProcessor insertRowForLogDB) {

		CassandraDataLoader cassandraDataLoader = new CassandraDataLoader();
		this.rowDataProcessor = insertRowForLogDB;
		getKafkaConsumer();
		String SERVER_NAME;
		try {
			SERVER_NAME = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			SERVER_NAME = "UnKnownHost";
		}
	}

	private void getKafkaConsumer() {
			ZK_IP = CassandraClient.getProperty("kafka.consumer.ip");
			ZK_PORT = CassandraClient.getProperty("kafka.consumer.port");
		String KAFKA_TOPIC = CassandraClient.getProperty("kafka.consumer.topic");
			KAFKA_GROUPID = CassandraClient.getProperty("kafka.consumer.group");
			LOG.info("Mesage Consumer: " + ZK_IP + ':' + ZK_PORT);
			MessageConsumer.topic = KAFKA_TOPIC.split(",");
			consumer = kafka.consumer.Consumer
					.createJavaConsumerConnector(createConsumerConfig());

	}

	private static String buildEndPoint(String ip, String portNo) {

		StringBuilder stringBuffer = new StringBuilder();
		String[] ips = ip.split(",");
		String[] ports = portNo.split(",");
		for (int count = 0; count < ips.length; count++) {

			if (stringBuffer.length() > 0) {
				stringBuffer.append(',');
			}

			if (count < ports.length) {
				stringBuffer.append(ips[count]).append(':').append(ports[count]);
			} else {
				stringBuffer.append(ips[count]).append(':').append(ports[0]);
			}
		}
		return stringBuffer.toString();
	}

	private static ConsumerConfig createConsumerConfig() {

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", MessageConsumer.buildEndPoint(ZK_IP, ZK_PORT));
		props.setProperty("group.id", KAFKA_GROUPID);
		props.setProperty("zookeeper.session.timeout.ms", "6000");
		props.setProperty("zookeeper.sync.time.ms", "200");
		props.setProperty("auto.commit.interval.ms", "1000");
		LOG.info("Kafka consumer config: " + ZK_IP + ':' + ZK_PORT + "::" + Arrays.toString(topic) + "::" + KAFKA_GROUPID);
		return new ConsumerConfig(props);

	}

	public void run() {
		/**
		 * get list of kafka stream from specific topic
		 */
		try {
			Map<String, Integer> topicCountMap = new HashMap<>();
			for (final String consumerTopic : topic) {
				topicCountMap.put(consumerTopic, 1);
			}
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			for (final String consumerTopic : topic) {
				LOG.info("Consumer topic : " + consumerTopic);
				service.submit(new ConsumeMessages(consumerTopic, consumerMap, rowDataProcessor));
			}

		} catch (Exception e) {
			LOG.error("Message Consumer failed in a loop:", e);
		}

	}

	/**
	 * Clean Shutdown
	 */
	public static void shutdownMessageConsumer() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			LOG.debug("Kafka Log Consumer unable to wait for 1000ms before it's shutdown");
		}
		consumer.shutdown();
	}
}
