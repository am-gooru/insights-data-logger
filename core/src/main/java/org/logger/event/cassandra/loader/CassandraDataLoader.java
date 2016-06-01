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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.ednovo.data.model.EventBuilder;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.logger.event.cassandra.loader.dao.BaseDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.EventsUpdateDAO;
import org.logger.event.cassandra.loader.dao.ReComputationDAO;
import org.logger.event.cassandra.loader.dao.ReportsGeneratorDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraDataLoader extends BaseDAOCassandraImpl {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

	private SimpleDateFormat minuteDateFormatter;

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	private KafkaLogProducer kafkaLogWriter;

	private ReportsGeneratorDAO reportsGeneratorDAO;

	private EventsUpdateDAO eventsUpdateDAO;
	
	private ReComputationDAO reComputationDAO;
	
	private BaseCassandraRepo baseDao;

	// private LTIServiceHandler ltiServiceHandler;

	/**
	 * Get Kafka properties from Environment
	 */
	public CassandraDataLoader() {
		this(null);
	}

	/**
	 * 
	 * @param configOptionsMap
	 */
	public CassandraDataLoader(Map<String, String> configOptionsMap) {
		init(configOptionsMap);
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
		reportsGeneratorDAO = ReportsGeneratorDAO.instance();
		eventsUpdateDAO = EventsUpdateDAO.instance();
		reComputationDAO = ReComputationDAO.instance();
		kafkaLogWriter = getKafkaLogProducer();
	}

	public void processMessage(EventBuilder event) {
		if (event.getFields() != null) {
			kafkaLogWriter.sendEventLog(event.getFields());
			LOG.info("Field : {}", event.getFields());
		}
		LOG.debug("pushed events for kafkaLogWriter");
		baseDao.insertEvents(event.getEventId(), event.getFields());
		LOG.debug("written in events column family");
		Date eventDateTime = new Date(event.getEndTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
		baseDao.insertEventsTimeline(eventRowKey, event.getEventId());
		LOG.debug("written in events_timeline column family");
		if (event.getEventName().matches(Constants.SESSION_ACTIVITY_EVENTS)) {
			reportsGeneratorDAO.eventProcessor(event);
			LOG.debug("eventProcessor completed");
		}
		if(event.getEventName().matches(Constants.RECOMPUTATION_EVENTS)){
			reComputationDAO.reComputeData(event);
			LOG.debug("reComputeData completed");
		}
		
		eventsUpdateDAO.eventsHandler(event);
		LOG.debug("eventsHandler completed");
	}
}
