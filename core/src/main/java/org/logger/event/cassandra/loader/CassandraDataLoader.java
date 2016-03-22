/*******************************************************************************
 * CassandraDataLoader.java
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
package org.logger.event.cassandra.loader;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.ednovo.data.model.EventBuilder;
import org.json.JSONException;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.logger.event.cassandra.loader.dao.LTIServiceHandler;
import org.logger.event.cassandra.loader.dao.MicroAggregatorDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraDataLoader {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

	private SimpleDateFormat minuteDateFormatter;

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	private KafkaLogProducer kafkaLogWriter;
	 
	private MicroAggregatorDAO liveAggregator;
	
	private BaseCassandraRepo baseDao;
	
	private LTIServiceHandler ltiServiceHandler;

	/**
	 * Get Kafka properties from Environment
	 */
	public CassandraDataLoader() {
		this(null);
		initializeKafkaModules();
	}

	/**
	 * 
	 * @param configOptionsMap
	 */
	public CassandraDataLoader(Map<String, String> configOptionsMap) {
		init(configOptionsMap);
		initializeKafkaModules();
	} 

	public static long getTimeFromUUID(UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

	/**
	 * 
	 * @param configOptionsMap
	 */
	private void init(Map<String, String> configOptionsMap) {
		this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		baseDao = BaseCassandraRepo.instance();
		//ltiServiceHandler = new LTIServiceHandler(baseDao);
		liveAggregator = MicroAggregatorDAO.instance();
	}	

	public void processMessage(EventBuilder event) {
		if (event.getFields() != null) {
			//kafkaLogWriter.sendEventLog(event.getFields());
			LOG.info("Field : {}" ,event.getFields());
		}
				
		baseDao.insertEvents(event.getEventId(), event.getFields());
		
		Date eventDateTime = new Date(event.getEndTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();

		baseDao.insertEventsTimeline(eventRowKey, event.getEventId());

		if (event.getEventName().matches(Constants.SESSION_ACTIVITY_EVENTS)) {
			liveAggregator.eventProcessor(event);
		} 
	}
	
	private void initializeKafkaModules(){
		// Log Writter producer IP
		if (getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER) != null && getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).size() > 0) {
			final String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_IP);
			final String KAFKA_LOG_WRITTER_PORT = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_PORT);
			final String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_TOPIC);
			final String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_PRODUCER_TYPE);
			kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT, KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
		}
	}
	
	public Map<String, String> getKafkaProperty(String propertyName) {
		return new HashMap<String, String>();
	}
	 
	public void updateStagingES(String startTime, String endTime, String customEventName, boolean isScheduledJob) throws ParseException {
	 
	}
}
