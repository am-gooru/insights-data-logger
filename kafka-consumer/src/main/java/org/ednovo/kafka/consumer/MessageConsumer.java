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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.mail.handlers.MailHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.netflix.astyanax.model.ColumnList;

public class MessageConsumer extends Thread implements Runnable {

	private CassandraDataLoader cassandraDataLoader;
	private BaseCassandraRepoImpl baseCassandraDAO;
	private static ConsumerConnector consumer;
	private DataProcessor rowDataProcessor;
	private MailHandler mailHandler;

	private static String topic;
	private static String ZK_IP;
	private static String ZK_PORT;
	private static String KAFKA_TOPIC;
	private static String KAFKA_GROUPID;
	private static String SERVER_NAME;
	
	private static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

	public MessageConsumer(DataProcessor insertRowForLogDB) {

		cassandraDataLoader = new CassandraDataLoader();
		mailHandler = new MailHandler();
		this.rowDataProcessor = insertRowForLogDB;
		getKafkaConsumer();
		try {
			SERVER_NAME = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			SERVER_NAME = "UnKnownHost";
		}
	}

	private void getKafkaConsumer() {
		Map<String, String> kafkaProperty = new HashMap<String, String>();
		kafkaProperty = cassandraDataLoader.getKafkaProperty("v2~kafka~consumer");
		ZK_IP = kafkaProperty.get("zookeeper_ip");
		ZK_PORT = kafkaProperty.get("zookeeper_portno");
		KAFKA_TOPIC = kafkaProperty.get("kafka_topic");
		KAFKA_GROUPID = kafkaProperty.get("kafka_groupid");
		logger.info("Mesage Consumer: " + ZK_IP + ":" + ZK_PORT);
		MessageConsumer.topic = KAFKA_TOPIC;
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	}

	private static String buildEndPoint(String ip, String portNo) {

		StringBuffer stringBuffer = new StringBuffer();
		String[] ips = ip.split(",");
		String[] ports = portNo.split(",");
		for (int count = 0; count < ips.length; count++) {

			if (stringBuffer.length() > 0) {
				stringBuffer.append(",");
			}

			if (count < ports.length) {
				stringBuffer.append(ips[count] + ":" + ports[count]);
			} else {
				stringBuffer.append(ips[count] + ":" + ports[0]);
			}
		}
		return stringBuffer.toString();
	}

	private static ConsumerConfig createConsumerConfig() {

		Properties props = new Properties();
		props.put("zookeeper.connect", MessageConsumer.buildEndPoint(ZK_IP, ZK_PORT));
		props.put("group.id", KAFKA_GROUPID);
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		logger.info("Kafka consumer config: " + ZK_IP + ":" + ZK_PORT + "::" + topic + "::" + KAFKA_GROUPID);
		return new ConsumerConfig(props);

	}

	public void run() {

		Integer noOfThread = 1;
		int loopCount = 0, status = 1, mailLoopCount = 0;
		int targetCount = 10;
		long sleepTime = 0;
		baseCassandraDAO = new BaseCassandraRepoImpl(new CassandraConnectionProvider());
		baseCassandraDAO.saveValue(ColumnFamily.JOB_TRACKER.getColumnFamily(), Constants.MONITOR_KAFKA_CONSUMER, Constants.STATUS, status);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		/**
		 * Iterate the loop for few times till the kafka get reconnected
		 */
		while (loopCount < targetCount) {
			try {
				/**
				 * Get config settings for kafka consumer
				 */
				ColumnList<String> columnList = baseCassandraDAO.readWithKey(ColumnFamily.JOB_TRACKER.getColumnFamily(), Constants.MONITOR_KAFKA_CONSUMER, 0);
				status = columnList.getIntegerValue(Constants.STATUS, 1);
				mailLoopCount = columnList.getIntegerValue(Constants.MAIL_LOOP_COUNT, 10);
				targetCount = columnList.getIntegerValue(Constants.THREAD_LOOP_COUNT, 10);
				sleepTime = columnList.getLongValue(Constants.THREAD_SLEEP_TIME, 10000L);

				/**
				 * will kill the thread,if the status is 0
				 */
				if (status == 0) {
					logger.error("kafka consumer stopped due to status:0");
					return;
				}

				/**
				 * will send the mail to developer for kafka failure
				 * notification
				 */
				if (loopCount != 0 && mailLoopCount != 0 && (loopCount % mailLoopCount) == 0) {
					mailHandler.sendKafkaNotification("Hi Team, \n \n Kafka consumer disconnected,so auto reconnect at server "+SERVER_NAME+" with loop count " + loopCount + " on " + new Date());
				}

				/**
				 * get list of kafka stream from specific topic
				 */
				topicCountMap.put(topic, new Integer(noOfThread));
				Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
				KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
				ConsumerIterator<byte[], byte[]> it = stream.iterator();
				/**
				 * process consumed data
				 */
				while (it.hasNext()) {
					String message = new String(it.next().message());
					Gson gson = new Gson();
					Map<String, String> messageMap = new HashMap<String, String>();
					try {
						messageMap = gson.fromJson(message, messageMap.getClass());
					} catch (Exception e) {
						ConsumerLogFactory.errorActivity.error(message);
						continue;
					}

					/**
					 * TODO We're only getting raw data now. We'll have to use
					 * the server IP as well for extra information.
					 **/
					if (messageMap != null && !messageMap.isEmpty()) {
						ConsumerLogFactory.activity.info(message);
						this.rowDataProcessor.processRow(messageMap.get("raw"));
					} else {
						ConsumerLogFactory.errorActivity.error(message);
					}
				}

			} catch (Exception e) {
				logger.error("Message Consumer:" + e);
			} finally {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					logger.error("Message Consumer Interrupted:" + e);
				}

				/**
				 * Restart the consumer thread
				 */
				consumer.shutdown();
				getKafkaConsumer();
			}
			loopCount++;
		}

		if (loopCount == targetCount) {
			mailHandler.sendKafkaNotification("Hi Team, \n \n Kafka consumer stopped at server "+SERVER_NAME+" with loop count " + loopCount + " on " + new Date());
		}
	}
}
