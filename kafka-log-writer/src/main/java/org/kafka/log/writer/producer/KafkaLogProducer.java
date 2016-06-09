/*******************************************************************************
 * KafkaLogProducer.java
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
package org.kafka.log.writer.producer;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.json.JSONObject;
import org.kafka.log.writer.consumer.KafkaLogConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaLogProducer
{
	private static final long serialVersionUID = 8483062836459978581L;
	private static final Logger LOG = LoggerFactory.getLogger(KafkaLogConsumer.class);
	private final SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyy HH:mm:ss:S");
	private Producer<String, String> producer;
	private String topic = "event-log-writer-dev";
	private String errorLogTopic = "error-event-log-writer";
	private final Properties props = new Properties();

	private KafkaLogProducer(){
	}

	public KafkaLogProducer(String kafkaIp, String port, String topic, String producerType) {
		init(kafkaIp, port, topic, producerType);
	}

	private void init(String kafkaIp, String port, String topic, String producerType) {
		this.topic = topic;
		this.errorLogTopic = "error"+topic;

		LOG.info("Kafka Data log writer producer config: "+ kafkaIp+ ':' +port+"::"+topic+"::"+producerType);
		LOG.info("Kafka Error File writer producer config: "+ kafkaIp+ ':' +port+"::"+errorLogTopic+"::"+producerType);
		props.setProperty("metadata.broker.list", KafkaLogProducer.buildEndPoint(kafkaIp, port));
		props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
		props.setProperty("request.required.acks", "1");
		props.setProperty("producer.type", producerType);


		try{
		producer = new Producer<>(new ProducerConfig(props));
		}
		catch (Exception e) {
			LOG.info("Error while initializing kafka : " + e);
		}
	}

	 private static String buildEndPoint(String ip, String portNo){

			StringBuilder stringBuffer  = new StringBuilder();
			String[] ips = ip.split(",");
			String[] ports = portNo.split(",");
			for( int count = 0; count<ips.length; count++){

				if(stringBuffer.length() > 0){
					stringBuffer.append(',');
				}

				if(count < ports.length){
					stringBuffer.append(ips[count]).append(':').append(ports[count]);
				}else{
					stringBuffer.append(ips[count]).append(':').append(ports[0]);
				}
			}
			return stringBuffer.toString();
		}

	public void sendEventLog(String eventLog) {
	Map<String, String> message = new HashMap<>();
	message.put("timestamp", dateFormatter.format(System.currentTimeMillis()));
	message.put("raw", eventLog);

	String messageAsJson = new JSONObject(message).toString();
	send(messageAsJson);
}

public void sendErrorEventLog(String eventLog) {
    Map<String, String> message = new HashMap<>();
    message.put("timestamp", dateFormatter.format(System.currentTimeMillis()));
    message.put("raw", eventLog);

    String messageAsJson = new JSONObject(message).toString();
    sendErrorEvent(messageAsJson);
}

 private void sendErrorEvent(String message) {
	 KeyedMessage<String, String> data = new KeyedMessage<>(errorLogTopic, message);
     producer.send(data);
 }


private void send(String message) {
	KeyedMessage<String, String> data = new KeyedMessage<>(topic, message);
	producer.send(data);
}

	public static void main(String args[]) {
		try{
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		 options.addOption( "kT", "kafkaTopic", true, "topic for the kafka server" );
	     options.addOption( "mess", "message", true, "message to be passed" );
	     CommandLine line = parser.parse( options, args );
	     String topic = "test";
	     String message = "data";
	     if(line.hasOption("kT")) {
             topic = line.getOptionValue( "kT");
         }
	     if(line.hasOption("mess")) {
	    	 message = line.getOptionValue( "mess");
         }

	     String kafkaIp = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_PRODUCER_IP");
	     String kafkaPort = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
	     String kafkaProducerType = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
	    // kafkaIp, String port, String topic, String producerType
		KafkaLogProducer producer = new KafkaLogProducer(kafkaIp,kafkaPort, topic,kafkaProducerType);
		producer.sendEventLog(message);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
