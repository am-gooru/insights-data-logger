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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.JSONDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.cassandra.loader.dao.ELSIndexerImpl;
import org.logger.event.cassandra.loader.dao.LTIServiceHandler;
import org.logger.event.cassandra.loader.dao.LiveDashBoardDAOImpl;
import org.logger.event.cassandra.loader.dao.MicroAggregatorDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.util.TimeUUIDUtils;

@Component
public class CassandraDataLoader implements Constants {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

	private SimpleDateFormat minuteDateFormatter;

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	private KafkaLogProducer kafkaLogWriter;
	
	private MicroAggregatorProducer microAggregator;
	
	@Autowired
	private MicroAggregatorDAOImpl liveAggregator;

	@Autowired
	private LiveDashBoardDAOImpl liveDashBoardDAOImpl;

	@Autowired
	private ELSIndexerImpl indexer;

	@Autowired
	private BaseCassandraRepoImpl baseDao;
	
	@Autowired
	private LTIServiceHandler ltiServiceHandler;

	@PostConstruct
	private void init() {
		this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		// micro Aggregator producer IP
		final String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty(V2_KAFKA_MICRO_PRODUCER).get(KAFKA_IP);
		final String KAFKA_AGGREGATOR_PORT = getKafkaProperty(V2_KAFKA_MICRO_PRODUCER).get(KAFKA_PORT);
		final String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty(V2_KAFKA_MICRO_PRODUCER).get(KAFKA_TOPIC);
		final String KAFKA_AGGREGATOR_TYPE = getKafkaProperty(V2_KAFKA_MICRO_PRODUCER).get(KAFKA_PRODUCER_TYPE);

		// Log Writter producer IP
		final String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty(V2_KAFKA_LOG_WRITER_PRODUCER).get(KAFKA_IP);
		final String KAFKA_LOG_WRITTER_PORT = getKafkaProperty(V2_KAFKA_LOG_WRITER_PRODUCER).get(KAFKA_PORT);
		final String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty(V2_KAFKA_LOG_WRITER_PRODUCER).get(KAFKA_TOPIC);
		final String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty(V2_KAFKA_LOG_WRITER_PRODUCER).get(KAFKA_PRODUCER_TYPE);
		kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT, KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
		microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT, KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
	}	

	public static long getTimeFromUUID(UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

	/**
	 * This is the main the method that process all the events.
	 * 
	 * @param event
	 * @throws JSONException
	 * @throws ConnectionException
	 * @throws IOException
	 * @throws GeoIp2Exception
	 */
	public void processMessage(Event event) {
		Map<String, Object> eventMap = JSONDeserializer.deserializeEvent(event);
		if (event.getFields() != null) {
			kafkaLogWriter.sendEventLog(event.getFields());

		}
		String eventName = event.getEventName();
		/**
		 * Calculate timespent in server side if more than two hours
		 */
		if (eventName.matches(PLAY_EVENTS)) {
			calculateTimespentAndViews(eventMap, event);
		}

		LOG.info("Field : {}" ,event.getFields());
		// TODO : This should be reject at validation stage.
		String apiKey = event.getApiKey() != null ? event.getApiKey() : DEFAULT_API_KEY;
		Map<String, Object> records = new HashMap<String, Object>();
		records.put(_EVENT_NAME, eventName);
		records.put(_API_KEY, apiKey);
		Collection<String> eventId = baseDao.getKey(ColumnFamilySet.DIMEVENTS.getColumnFamily(), records);

		if (eventId == null || eventId.isEmpty()) {
			UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			records.put(_EVENT_ID, uuid.toString());
			String key = apiKey + SEPERATOR + uuid.toString();
			baseDao.saveBulkList(ColumnFamilySet.DIMEVENTS.getColumnFamily(), key, records);
		}

		String eventKeyUUID = baseDao.saveEvent(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), null, event);

		if (eventKeyUUID == null) {
			return;
		}

		Date eventDateTime = new Date(event.getEndTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();

		baseDao.updateTimelineObject(ColumnFamilySet.EVENTTIMELINE.getColumnFamily(), eventRowKey, eventKeyUUID.toString(), event);

		if (eventName.matches(SESSION_ACTIVITY_EVENTS)) {
			liveAggregator.eventProcessor(eventMap);
		} else if(eventName.equalsIgnoreCase(LTI_OUTCOME)){
			ltiServiceHandler.ltiEventProcess(eventName, eventMap);
		}
		
		/*liveDashBoardDAOImpl.realTimeMetricsCounter(eventMap);

		if (DataLoggerCaches.getCache().get(VIEW_EVENTS).contains(eventName)) {
			liveDashBoardDAOImpl.addContentForPostViews(eventMap);
		}
*/
		if (DataLoggerCaches.getCanRunIndexing()) {
			//indexer.indexEvents(event.getFields());
		}

		/**
		 * To be Re-enable
		 * 
		 *if (event.getFields() != null) {
		 *	microAggregator.sendEventForAggregation(event.getFields());
		 *} 
		 * liveDashBoardDAOImpl.findDifferenceInCount(eventMap);
		 * 
		 * liveDashBoardDAOImpl.addApplicationSession(eventMap);
		 * 
		 * liveDashBoardDAOImpl.saveGeoLocations(eventMap);
		 * 
		 * 
		 * if(pushingEvents.contains(eventMap.get(EVENT_NAME))){ liveDashBoardDAOImpl .pushEventForAtmosphere(cache.get(ATMOSPHERENDPOINT),eventMap); }
		 * 
		 * if(eventMap.get(EVENT_NAME).equalsIgnoreCase(LoaderConstants.CRPV1 .getName())){ liveDashBoardDAOImpl.pushEventForAtmosphereProgress( atmosphereEndPoint, eventMap); }
		 */

	}

	/**
	 * 
	 * @param eventData
	 *            Update the event is completion status
	 * @throws ConnectionException
	 *             If the host is unavailable
	 */
	private void calculateTimespentAndViews(Map<String, Object> eventMap, Event event) {
		try {
			long views = 1L;
			long timeSpent = (event.getEndTime() - event.getStartTime());
			String collectionType = eventMap.containsKey(COLLECTION_TYPE) ? (String)eventMap.get(COLLECTION_TYPE) : null;
			String eventType = eventMap.containsKey(TYPE) ? (String)eventMap.get(TYPE) : null;
			if((START.equals(eventType) && ASSESSMENT.equalsIgnoreCase(collectionType)) || (STOP.equals(eventType) && COLLECTION.equalsIgnoreCase(collectionType))){
				views = 0L;
			}
			
			if(timeSpent > 7200000 || timeSpent < 0){
				timeSpent = 7200000;
			}
			JSONObject eventMetrics = new JSONObject(event.getMetrics());
			eventMetrics.put(TOTALTIMEINMS, timeSpent);
			eventMetrics.put(VIEWS_COUNT, views);
			event.setMetrics(eventMetrics);
			
			JSONObject eventFields = new JSONObject(event.getFields());
			eventFields.put(METRICS, eventMetrics.toString());
			event.setFields(eventFields.toString());
			
			eventMap.put(VIEWS_COUNT, views);
			eventMap.put(TOTALTIMEINMS, timeSpent);
		} catch (Exception e) {
			LOG.error("Exeption while calculting timespent & views:", e);
		}
	}
	

	public void updateActivityCompletion(String userUid, ColumnList<String> activityRow, String eventId, Map<String, Object> timeMap) {
		Long startTime = activityRow.getLongValue(_START_TIME, 0L), endTime = activityRow.getLongValue(_END_TIME, 0L);
		String eventType = null;
		JsonElement jsonElement = null;
		JsonObject existingEventObj = null;
		String existingColumnName = null;

		if (activityRow.getStringValue(_EVENT_TYPE, null) != null) {
			eventType = activityRow.getStringValue(_EVENT_TYPE, null);
		}

		long timeInMillisecs = 0L;
		if (endTime != null && startTime != null) {
			timeInMillisecs = endTime - startTime;
		}

		if (!StringUtils.isEmpty(eventType) && userUid != null) {
			Map<String, Object> existingRecord = baseDao.isEventIdExists(ColumnFamilySet.ACITIVITYSTREAM.getColumnFamily(), userUid, eventId);
			if (existingRecord.get("isExists").equals(true) && existingRecord.get("jsonString").toString() != null) {
				jsonElement = new JsonParser().parse(existingRecord.get("jsonString").toString());
				existingEventObj = jsonElement.getAsJsonObject();
				if (COMPLETED_EVENT.equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType)) {
					existingColumnName = existingRecord.get("existingColumnName").toString();
					startTime = existingEventObj.get(_START_TIME).getAsLong();
				} else {
					endTime = existingEventObj.get(_END_TIME).getAsLong();
				}
			}

			// Time taken for the event in milliseconds derived from the start /
			// stop events.
			if (endTime != null && startTime != null) {
				timeInMillisecs = endTime - startTime;
			}
			if (timeInMillisecs > 1147483647) {
				// When time in Milliseconds is very very huge, set to min time
				// to serve the call.
				timeInMillisecs = 30;
				// Since this is an error condition, log it.
			}
		}
		timeMap.put(START_TIME, startTime);
		timeMap.put(END_TIME, endTime);
		timeMap.put("event_type", eventType);
		timeMap.put("existingColumnName", existingColumnName);
		timeMap.put("timeSpent", timeInMillisecs);

	}

	public void runMicroAggregation(String startTime, String endTime) {
		LOG.debug("start the static loader");
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put(START_TIME, startTime);
			jsonObject.put(END_TIME, endTime);
		} catch (JSONException e) {
			LOG.error("Exception:" , e);
		}
		microAggregator.sendEventForStaticAggregation(jsonObject.toString());
	}

	/**
	 * 
	 * @param eventName
	 * @param apiKey
	 * @return
	 */
	public boolean createEvent(String eventName, String applicationKey) {

		Map<String, Object> records = new HashMap<String, Object>();
		String apiKey = applicationKey == null ? DEFAULT_API_KEY : applicationKey; 
		records.put("api_key", apiKey);
		records.put("event_name", eventName);
		if (baseDao.isValueExists(ColumnFamilySet.DIMEVENTS.getColumnFamily(), records)) {

			UUID eventId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			records.put("event_id", eventId.toString());
			String key = apiKey + SEPERATOR + eventId.toString();
			baseDao.saveBulkList(ColumnFamilySet.DIMEVENTS.getColumnFamily(), key, records);
			return true;
		}
		return false;
	}

	public boolean validateSchedular() {
		return DataLoggerCaches.getCanRunScheduler();
	}

	/**
	 * Index all events between a given start and end times.
	 * 
	 * @param startTime
	 * @param endTime
	 * @param customEventName
	 * @param isScheduledJob
	 *            true if called from scheduler. false when called from real-time indexing.
	 * @throws ParseException
	 */
	public void updateStagingES(String startTime, String endTime, String customEventName, boolean isScheduledJob) throws ParseException {

		if (isScheduledJob) {
			// Called from Scheduled job. Mark the status as in-progress so that
			// no other activity will overlap.
			baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_STATUS, DEFAULT_COLUMN, INPROGRESS);
			baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN, "" + 0);
		}

		String timeLineKey = null;
		final long batchEndDate = minuteDateFormatter.parse(endTime).getTime();

		for (long batchStartDate = minuteDateFormatter.parse(startTime).getTime(); batchStartDate < batchEndDate;) {
			String currentDate = minuteDateFormatter.format(new Date(batchStartDate));
			LOG.info("Processing Date : {}", currentDate);
			if (StringUtils.isBlank(customEventName)) {
				timeLineKey = currentDate.toString();
			} else {
				timeLineKey = currentDate.toString() + "~" + customEventName;
			}

			// Read Event Time Line for event keys and create as a Collection
			ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamilySet.EVENTTIMELINE.getColumnFamily(), timeLineKey);

			if (eventUUID != null && !eventUUID.isEmpty()) {
				for (Column<String> column : eventUUID) {
					LOG.debug("eventDetailUUID  : " + column.getStringValue());
					ColumnList<String> event = baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), column.getStringValue());
					indexer.indexEvents(event.getStringValue("fields", null));
				}

			}
			// Incrementing time - one minute
			batchStartDate = new Date(batchStartDate).getTime() + 60000;

			if (isScheduledJob) {
				baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_LAST_UPDATED, DEFAULT_COLUMN, "" + minuteDateFormatter.format(new Date(batchStartDate)));
			}
		}

		if (isScheduledJob) {
			baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_STATUS, DEFAULT_COLUMN, COMPLETED);
		}

		LOG.debug("Indexing completed..........");
	}

	/**
	 * Indexing content
	 * @param ids
	 * @throws Exception 
	 */
    public void indexResource(String ids) throws Exception{
    	indexer.indexResource(ids);
    }
    /**
     * Indexing user using user uuid.
     * @param ids
     * @throws Exception
     */
    public void indexUser(String ids) throws Exception{
    	for(String userId : ids.split(",")){
    		indexer.getUserAndIndex(userId);
    	}
    }
    /**
     * Indexing activity using event id
     * @param ids
     * @throws Exception
     */
    public void indexEvent(String ids) throws Exception{
    	for(String eventId : ids.split(",")){
    		ColumnList<String> event =  baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventId);
    		if(event.size() > 0){
    			indexer.indexEvents(event.getStringValue("fields", null));
    		}else{
    			LOG.error("Invalid event id:" + eventId);
    		}
    	}
    }
    /**
     * Indexing taxonomy index
     * @param key
     * @throws Exception
     */
    public void indexTaxonomy(String key) throws Exception{
    	indexer.indexTaxonomy(key);
    }
    
	/**
	 * 
	 * @param eventData
	 * @return
	 */

	public Map<String, String> getKafkaProperty(String propertyName) {
		return DataLoggerCaches.getKafkaConfigurationCache().get(propertyName);
	}
}
