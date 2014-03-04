/*******************************************************************************
 * KafkaLogConsumer.java
 * kafka-log-writer
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
package org.kafka.log.writer.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class KafkaLogConsumer extends Thread {

	private final ConsumerConnector consumer;
	private static String topic;
	private static String KAFKA_PRODUCER_TYPE;
	private static String KAFKA_TOPIC;
	private static String KAFKA_FILE_TOPIC;
	private static String KAFKA_ZK_PORT;
	private static String KAFKA_PORT;
	private static String KAFKA_IP;
	private static String KAFKA_GROUPID;
	private static String KAFKA_FILE_GROUPID;
	static final Logger logger = LoggerFactory.getLogger(KafkaLogConsumer.class);
	private static final Logger activityLogger = LoggerFactory.getLogger("activityLog");
	
	public KafkaLogConsumer(String topic)
	  {
	    
	    KAFKA_IP = System.getenv("INSIGHTS_KAFKA_IP");
        KAFKA_PORT = System.getenv("INSIGHTS_KAFKA_PORT");
        KAFKA_ZK_PORT = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
        KAFKA_TOPIC = System.getenv("INSIGHTS_KAFKA_TOPIC");
        KAFKA_PRODUCER_TYPE = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
        KAFKA_GROUPID = System.getenv("INSIGHTS_KAFKA_GROUPID");
        KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
        KAFKA_FILE_GROUPID = System.getenv("INSIGHTS_KAFKA_FILE_GROUPID");
        
        this.topic = KAFKA_FILE_TOPIC;
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	  }

	  private static ConsumerConfig createConsumerConfig()
	  {
	    Properties props = new Properties();
	    props.put("zk.connect", KAFKA_IP + ":" + KAFKA_ZK_PORT);
	    props.put("groupid", KAFKA_FILE_GROUPID);
	    props.put("zk.sessiontimeout.ms", "10000");
	    props.put("zk.synctime.ms", "200");
	    props.put("autocommit.interval.ms", "1000");
	    
	    logger.info("Kafka File writer consumer config: "+ KAFKA_IP+":"+KAFKA_ZK_PORT+"::"+ topic+"::"+KAFKA_FILE_GROUPID);

	    return new ConsumerConfig(props);

	  }
	 
	  public void run() {
	    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, new Integer(1));
	    Map<String, List<KafkaMessageStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	    KafkaMessageStream<Message> stream =  consumerMap.get(topic).get(0);
	    ConsumerIterator<Message> it = stream.iterator();
	    while(it.hasNext())
	    {
	    	String message = ExampleUtils.getMessage(it.next());
	    	Gson gson = new Gson();
	    	Map<String, String> messageMap = new HashMap<String, String>();
	    	try {
	    		messageMap = gson.fromJson(message, messageMap.getClass());
			} catch (Exception e) {
				logger.error("Message Consumer Error: "+ e.getMessage());
				continue; 
			}
	    	
	    	//TODO We're only getting raw data now. We'll have to use the server IP as well for extra information.
	    	if(messageMap != null)
	    	{
	    		String eventJson = (String)messageMap.get("raw");	    		
	    		//Write the consumed JSON to Log file.
	    		activityLogger.info(eventJson);
	    	}
	    	else
	    	{
	    		logger.error("Message Consumer Error messageMap : No data found");
	    		continue;
	    	}
	    }
	  }
}
