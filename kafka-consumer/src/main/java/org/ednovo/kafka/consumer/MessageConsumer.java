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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class MessageConsumer extends Thread
{
	CassandraDataLoader cassandraDataLoader = new CassandraDataLoader();
	private final ConsumerConnector consumer;
	private static String topic;
	private DataProcessor rowDataProcessor;
	private static String ZK_IP;
	private static String ZK_PORT;
	private static String KAFKA_TOPIC;
	private static String KAFKA_GROUPID;
  static final Logger LOG = LoggerFactory.getLogger(MessageConsumer.class);
  
  public MessageConsumer(DataProcessor insertRowForLogDB)
  {
	  Map<String, String> kafkaProperty = new HashMap<String, String>();
		kafkaProperty = cassandraDataLoader.getKafkaProperty("v2~kafka~consumer");
		ZK_IP = kafkaProperty.get("zookeeper_ip");
		ZK_PORT = kafkaProperty.get("zookeeper_portno");
		KAFKA_TOPIC = kafkaProperty.get("kafka_topic");
		KAFKA_GROUPID = kafkaProperty.get("kafka_groupid");
		LOG.info("Mesage Consumer: " + ZK_IP + ":" + ZK_PORT);
		this.topic = KAFKA_TOPIC;
		this.rowDataProcessor = insertRowForLogDB;

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    
  }

  public static String buildEndPoint(String ip, String portNo){
		
		StringBuffer stringBuffer  = new StringBuffer();
		String[] ips = ip.split(",");
		String[] ports = portNo.split(",");
		for( int count = 0; count<ips.length; count++){
			
			if(stringBuffer.length() > 0){
				stringBuffer.append(",");
			}
			
			if(count < ports.length){
				stringBuffer.append(ips[count]+":"+ports[count]);
			}else{
				stringBuffer.append(ips[count]+":"+ports[0]);
			}
		}
		return stringBuffer.toString();
	}
  
  private static ConsumerConfig createConsumerConfig(){
	  
	  Properties props = new Properties();
		props.put("zookeeper.connect", MessageConsumer.buildEndPoint(ZK_IP, ZK_PORT));
		props.put("group.id", KAFKA_GROUPID);
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		LOG.info("Kafka consumer config: " + ZK_IP + ":" + ZK_PORT + "::" + topic + "::" + KAFKA_GROUPID);

    return new ConsumerConfig(props);

  }
 
  public void run() {
	  Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		Integer noOfThread = 1;
		topicCountMap.put(topic, new Integer(noOfThread));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			String message = new String(it.next().message());
    	Gson gson = new Gson();
    	Map<String, String> messageMap = new HashMap<String, String>();
    	try {
    		messageMap = gson.fromJson(message, messageMap.getClass());
		} catch (Exception e) {
			LOG.error("Message Consumer Error: "+ e.getMessage());
			continue; 
		}
    	
    	//TODO We're only getting raw data now. We'll have to use the server IP as well for extra information.
    	if(messageMap != null)
    	{
    		this.rowDataProcessor.processRow(messageMap.get("raw"));
    		LOG.error("Message Consumer messageMap :\n" + messageMap.get("raw"));
    	}
    	else
    	{
    		LOG.error("Message Consumer Error messageMap : No data found");
    		continue;
    	}
    }
  }
}
