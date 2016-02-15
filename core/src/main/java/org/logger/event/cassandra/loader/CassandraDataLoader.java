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

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.JSONDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.logger.event.cassandra.loader.dao.ELSIndexerImpl;
import org.logger.event.cassandra.loader.dao.LTIServiceHandler;
import org.logger.event.cassandra.loader.dao.LiveDashBoardDAOImpl;
import org.logger.event.cassandra.loader.dao.MicroAggregatorDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.util.TimeUUIDUtils;

import flexjson.JSONSerializer;

public class CassandraDataLoader {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	private SimpleDateFormat minuteDateFormatter;

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	private KafkaLogProducer kafkaLogWriter;
	
	private MicroAggregatorProducer microAggregator;
	
	private MicroAggregatorDAO liveAggregator;

	private LiveDashBoardDAOImpl liveDashBoardDAOImpl;

	private ELSIndexerImpl indexer;

	private BaseCassandraRepo baseDao;
	
	private LTIServiceHandler ltiServiceHandler;

	/**
	 * Get Kafka properties from Environment
	 */
	public CassandraDataLoader() {
		this(null);

		// micro Aggregator producer IP
		final String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_IP);
		final String KAFKA_AGGREGATOR_PORT = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_PORT);
		final String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_TOPIC);
		final String KAFKA_AGGREGATOR_TYPE = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_PRODUCER_TYPE);

		// Log Writter producer IP
		final String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_IP);
		final String KAFKA_LOG_WRITTER_PORT = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_PORT);
		final String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_TOPIC);
		final String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_PRODUCER_TYPE);
		kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT, KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
		microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT, KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
	}

	/**
	 * 
	 * @param configOptionsMap
	 */
	public CassandraDataLoader(Map<String, String> configOptionsMap) {
		init(configOptionsMap);
		// micro Aggregator producer IP
		final String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_IP);
		final String KAFKA_AGGREGATOR_PORT = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_PORT);
		final String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_TOPIC);
		final String KAFKA_AGGREGATOR_TYPE = getKafkaProperty(Constants.V2_KAFKA_MICRO_PRODUCER).get(Constants.KAFKA_PRODUCER_TYPE);

		// Log Writter producer IP
		final String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_IP);
		final String KAFKA_LOG_WRITTER_PORT = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_PORT);
		final String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_TOPIC);
		final String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty(Constants.V2_KAFKA_LOG_WRITER_PRODUCER).get(Constants.KAFKA_PRODUCER_TYPE);

		microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT, KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
		kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT, KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);	
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
		this.liveDashBoardDAOImpl = new LiveDashBoardDAOImpl();
		indexer = new ELSIndexerImpl();
		baseDao = BaseCassandraRepo.instance();
		ltiServiceHandler = new LTIServiceHandler(baseDao);
		liveAggregator = MicroAggregatorDAO.instance();
	}	
	/**
	 * 
	 * @param fields
	 * @param startTime
	 * @param userAgent
	 * @param userIp
	 * @param endTime
	 * @param apiKey
	 * @param eventName
	 * @param gooruOId
	 * @param contentId
	 * @param query
	 * @param gooruUId
	 * @param userId
	 * @param gooruId
	 * @param type
	 * @param parentEventId
	 * @param context
	 * @param reactionType
	 * @param organizationUid
	 * @param timeSpentInMs
	 * @param answerId
	 * @param attemptStatus
	 * @param trySequence
	 * @param requestMethod
	 * @param eventId
	 * 
	 *            Generate EventData Object
	 */
	public void handleLogMessage(String fields, Long startTime, String userAgent, String userIp, Long endTime, String apiKey, String eventName, String gooruOId, String contentId, String query,
			String gooruUId, String userId, String gooruId, String type, String parentEventId, String context, String reactionType, String organizationUid, Long timeSpentInMs, int[] answerId,
			int[] attemptStatus, int[] trySequence, String requestMethod, String eventId) {
		EventData eventData = new EventData();
		eventData.setEventId(eventId);
		eventData.setStartTime(startTime);
		eventData.setEndTime(endTime);
		eventData.setUserAgent(userAgent);
		eventData.setEventName(eventName);
		eventData.setUserIp(userIp);
		eventData.setApiKey(apiKey);
		eventData.setFields(fields);
		eventData.setGooruOId(gooruOId);
		eventData.setContentId(contentId);
		eventData.setQuery(query);
		eventData.setGooruUId(gooruUId);
		eventData.setUserId(userId);
		eventData.setGooruId(gooruId);
		eventData.setOrganizationUid(organizationUid);
		eventData.setType(type);
		eventData.setContext(context);
		eventData.setParentEventId(parentEventId);
		eventData.setTimeSpentInMs(timeSpentInMs);
		eventData.setAnswerId(answerId);
		eventData.setAttemptStatus(attemptStatus);
		eventData.setAttemptTrySequence(trySequence);
		eventData.setRequestMethod(requestMethod);
		handleLogMessage(eventData);
	}

	/**
	 * 
	 * @param eventData
	 *            process EventData Object
	 * @exception ConnectionException
	 *                If the host is unavailable
	 * 
	 */
	public void handleLogMessage(EventData eventData) {

		// Increment Resource view counts for real time

		this.getAndSetAnswerStatus(eventData);

		if (eventData.getEventName().equalsIgnoreCase(LoaderConstants.CR.getName())) {
			eventData.setQuery(eventData.getReactionType());
		}

		if (StringUtils.isEmpty(eventData.getFields()) || eventData.getStartTime() == null) {
			return;
		}
		if (StringUtils.isEmpty(eventData.getEventType()) && !StringUtils.isEmpty(eventData.getType())) {
			eventData.setEventType(eventData.getType());
		}

		try {
			ColumnList<String> existingRecord = null;
			Long startTimeVal = null;
			Long endTimeVal = null;

			if (eventData.getEventId() != null) {
				existingRecord = baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData.getEventId());
				if (existingRecord != null && !existingRecord.isEmpty()) {
					if ("start".equalsIgnoreCase(eventData.getEventType())) {
						startTimeVal = existingRecord.getLongValue("start_time", null);
					}
					if ("stop".equalsIgnoreCase(eventData.getEventType())) {
						endTimeVal = existingRecord.getLongValue("end_time", null);
					}
					if (startTimeVal == null && endTimeVal == null) {
						// This is a duplicate event. Don't do anything!
						return;
					}
				}
			}
			Map<String, Object> records = new HashMap<String, Object>();
			records.put("event_name", eventData.getEventName());
			records.put("api_key", eventData.getApiKey() != null ? eventData.getApiKey() : Constants.DEFAULT_API_KEY);
			Collection<String> existingEventRecord = baseDao.getKey(ColumnFamilySet.DIMEVENTS.getColumnFamily(), records);

			if (existingEventRecord == null || existingEventRecord.isEmpty()) {
				LOG.info("Please add new event in to events table ");
				return;
			}

			updateEventCompletion(eventData);

			String eventKeyUUID = updateEvent(eventData);
			if (eventKeyUUID == null) {
				return;
			}
			/**
			 * write the JSON to Log file using kafka log writer module in aysnc mode. This will store/write all data to activity log file in log/event_api_logs/activity.log
			 */
			if (eventData.getFields() != null) {
				baseDao.saveEvent(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData);
				kafkaLogWriter.sendEventLog(eventData.getFields());
				LOG.info("CORE: Writing to activity log - :" + eventData.getFields().toString());
			}

			// Insert into event_timeline column family
			Date eventDateTime = new Date(eventData.getStartTime());
			String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
			if (eventData.getEventType() == null || !eventData.getEventType().equalsIgnoreCase(Constants.COMPLETED_EVENT)) {
				eventData.setEventKeyUUID(eventKeyUUID.toString());
				baseDao.updateTimeline(ColumnFamilySet.EVENTTIMELINE.getColumnFamily(), eventData, eventRowKey);
			}
			try {
				updateActivityStream(eventData.getEventId());
			} catch (JSONException e) {
				LOG.info("Json Exception while saving Activity Stream via old event format {}", e);
			}
		} catch (ConnectionException e) {
			LOG.info("Exception while processing update for rowkey {} ", e);
		}
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
		if (eventName.matches(Constants.PLAY_EVENTS)) {
			calculateTimespentAndViews(eventMap, event);
		}

		LOG.info("Field : {}" ,event.getFields());
		// TODO : This should be reject at validation stage.
		String apiKey = event.getApiKey() != null ? event.getApiKey() : Constants.DEFAULT_API_KEY;
		Map<String, Object> records = new HashMap<String, Object>();
		records.put(Constants._EVENT_NAME, eventName);
		records.put(Constants._API_KEY, apiKey);
		Collection<String> eventId = baseDao.getKey(ColumnFamilySet.DIMEVENTS.getColumnFamily(), records);

		if (eventId == null || eventId.isEmpty()) {
			UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			records.put(Constants._EVENT_ID, uuid.toString());
			String key = apiKey + Constants.SEPERATOR + uuid.toString();
			baseDao.saveBulkList(ColumnFamilySet.DIMEVENTS.getColumnFamily(), key, records);
		}

		String eventKeyUUID = baseDao.saveEvent(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), null, event);

		if (eventKeyUUID == null) {
			return;
		}

		Date eventDateTime = new Date(event.getEndTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();

		baseDao.updateTimelineObject(ColumnFamilySet.EVENTTIMELINE.getColumnFamily(), eventRowKey, eventKeyUUID.toString(), event);

		if (eventName.matches(Constants.SESSION_ACTIVITY_EVENTS)) {
			liveAggregator.eventProcessor(eventMap);
		} else if(eventName.equalsIgnoreCase(Constants.LTI_OUTCOME)){
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
			String collectionType = eventMap.containsKey(Constants.COLLECTION_TYPE) ? (String)eventMap.get(Constants.COLLECTION_TYPE) : null;
			String eventType = eventMap.containsKey(Constants.TYPE) ? (String)eventMap.get(Constants.TYPE) : null;
			if((Constants.START.equals(eventType) && Constants.ASSESSMENT.equalsIgnoreCase(collectionType)) || (Constants.STOP.equals(eventType) && Constants.COLLECTION.equalsIgnoreCase(collectionType))){
				views = 0L;
			}
			
			if(timeSpent > 7200000 || timeSpent < 0){
				timeSpent = 7200000;
			}
			JSONObject eventMetrics = new JSONObject(event.getMetrics());
			eventMetrics.put(Constants.TOTALTIMEINMS, timeSpent);
			eventMetrics.put(Constants.VIEWS_COUNT, views);
			event.setMetrics(eventMetrics);
			
			JSONObject eventFields = new JSONObject(event.getFields());
			eventFields.put(Constants.METRICS, eventMetrics.toString());
			event.setFields(eventFields.toString());
			
			eventMap.put(Constants.VIEWS_COUNT, views);
			eventMap.put(Constants.TOTALTIMEINMS, timeSpent);
		} catch (Exception e) {
			LOG.error("Exeption while calculting timespent & views:", e);
		}
	}
	

	public void updateActivityCompletion(String userUid, ColumnList<String> activityRow, String eventId, Map<String, Object> timeMap) {
		Long startTime = activityRow.getLongValue(Constants._START_TIME, 0L), endTime = activityRow.getLongValue(Constants._END_TIME, 0L);
		String eventType = null;
		JsonElement jsonElement = null;
		JsonObject existingEventObj = null;
		String existingColumnName = null;

		if (activityRow.getStringValue(Constants._EVENT_TYPE, null) != null) {
			eventType = activityRow.getStringValue(Constants._EVENT_TYPE, null);
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
				if (Constants.COMPLETED_EVENT.equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType)) {
					existingColumnName = existingRecord.get("existingColumnName").toString();
					startTime = existingEventObj.get(Constants._START_TIME).getAsLong();
				} else {
					endTime = existingEventObj.get(Constants._END_TIME).getAsLong();
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
		timeMap.put(Constants.START_TIME, startTime);
		timeMap.put(Constants.END_TIME, endTime);
		timeMap.put("event_type", eventType);
		timeMap.put("existingColumnName", existingColumnName);
		timeMap.put("timeSpent", timeInMillisecs);

	}

	public void runMicroAggregation(String startTime, String endTime) {
		LOG.debug("start the static loader");
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put(Constants.START_TIME, startTime);
			jsonObject.put(Constants.END_TIME, endTime);
		} catch (JSONException e) {
			LOG.error("Exception:" + e);
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
		String apiKey = applicationKey == null ? Constants.DEFAULT_API_KEY : applicationKey; 
		records.put("api_key", apiKey);
		records.put("event_name", eventName);
		if (baseDao.isValueExists(ColumnFamilySet.DIMEVENTS.getColumnFamily(), records)) {

			UUID eventId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			records.put("event_id", eventId.toString());
			String key = apiKey + Constants.SEPERATOR + eventId.toString();
			baseDao.saveBulkList(ColumnFamilySet.DIMEVENTS.getColumnFamily(), key, records);
			return true;
		}
		return false;
	}

	public boolean validateSchedular() {
		return DataLoggerCaches.getCanRunScheduler();
	}

	/**
	 * 
	 * @param eventData
	 */
	private void getAndSetAnswerStatus(EventData eventData) {
		if (eventData.getEventName().equalsIgnoreCase(LoaderConstants.CQRPD.getName()) || eventData.getEventName().equalsIgnoreCase(LoaderConstants.QPD.getName())) {
			String answerStatus = null;
			if (eventData.getAttemptStatus().length == 0) {
				answerStatus = LoaderConstants.SKIPPED.getName();
				eventData.setAttemptFirstStatus(answerStatus);
				eventData.setAttemptNumberOfTrySequence(eventData.getAttemptTrySequence().length);
			} else {
				if (eventData.getAttemptStatus()[0] == 1) {
					answerStatus = LoaderConstants.CORRECT.getName();
				} else if (eventData.getAttemptStatus()[0] == 0) {
					answerStatus = LoaderConstants.INCORRECT.getName();
				}
				eventData.setAttemptFirstStatus(answerStatus);
				eventData.setAttemptNumberOfTrySequence(eventData.getAttemptTrySequence().length);
				eventData.setAnswerFirstId(eventData.getAnswerId()[0]);
			}

		}
	}

	private void updateEventCompletion(EventData eventData) throws ConnectionException {

		Long endTime = eventData.getEndTime(), startTime = eventData.getStartTime();
		long timeInMillisecs = 0L;
		if (endTime != null && startTime != null) {
			timeInMillisecs = endTime - startTime;
		}
		boolean eventComplete = false;

		eventData.setTimeInMillSec(timeInMillisecs);

		if (StringUtils.isEmpty(eventData.getEventId())) {
			return;
		}

		if (StringUtils.isEmpty(eventData.getEventType()) && !StringUtils.isEmpty(eventData.getType())) {
			eventData.setEventType(eventData.getType());
		}

		if (!StringUtils.isEmpty(eventData.getEventType())) {
			ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData.getEventId());
			if (existingRecord != null && !existingRecord.isEmpty()) {
				if ("stop".equalsIgnoreCase(eventData.getEventType())) {
					startTime = existingRecord.getLongValue("start_time", null);
					// Update startTime with existingRecord, IF
					// existingRecord.startTime < startTime
				} else {
					endTime = existingRecord.getLongValue("end_time", null);
					// Update endTime with existing record IF
					// existingRecord.endTime > endTime
				}
				eventComplete = true;
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

		eventData.setStartTime(startTime);
		eventData.setEndTime(endTime);

		if (eventComplete) {
			eventData.setTimeInMillSec(timeInMillisecs);
			eventData.setEventType(Constants.COMPLETED_EVENT);
			eventData.setEndTime(endTime);
			eventData.setStartTime(startTime);
		}

		if (!StringUtils.isEmpty(eventData.getParentEventId())) {
			ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId());
			if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
				Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
				baseDao.saveLongValue(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), "end_time", endTime);
				baseDao.saveLongValue(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), "time_spent_in_millis", (endTime - parentStartTime));

			}
		}

	}

	/**
	 * 
	 * @param eventId
	 * @throws JSONException
	 */
	private void updateActivityStream(String eventId) throws JSONException {

		if (eventId != null) {

			Map<String, String> rawMap = new HashMap<String, String>();
			String apiKey = null;
			String userName = null;
			String dateId = null;
			String userUid = null;
			String contentGooruId = null;
			String parentGooruId = null;
			String organizationUid = null;
			Date endDate = new Date();

			ColumnList<String> activityRow = baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventId);
			if (activityRow != null) {
				SimpleDateFormat minuteDateFormatter = new SimpleDateFormat(Constants.MINUTE_DATE_FORMATTER);
				HashMap<String, Object> activityMap = new HashMap<String, Object>();
				Map<String, Object> eventMap = new HashMap<String, Object>();
				if (activityRow.getLongValue(Constants._END_TIME, null) != null) {
					endDate = new Date(activityRow.getLongValue(Constants._END_TIME, null));
				} else {
					endDate = new Date(activityRow.getLongValue(Constants._START_TIME, null));
				}
				dateId = minuteDateFormatter.format(endDate).toString();
				Map<String, Object> timeMap = new HashMap<String, Object>();

				// Get userUid
				if (rawMap != null && rawMap.get(Constants.GOORUID) != null) {
					try {
						userUid = rawMap.get(Constants.GOORUID);
						Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamilySet.DIMUSER.getColumnFamily(), Constants._GOORU_UID, userUid);
						for (Row<String, String> userDetail : userDetails) {
							userName = userDetail.getColumns().getStringValue("username", null);
						}
					} catch (Exception e) {
						LOG.info("Error while fetching User uid ");
					}
				} else if (activityRow.getStringValue(Constants._GOORU_UID, null) != null) {
					try {
						userUid = activityRow.getStringValue(Constants._GOORU_UID, null);
						Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamilySet.DIMUSER.getColumnFamily(), Constants._GOORU_UID, activityRow.getStringValue(Constants._GOORU_UID, null));
						for (Row<String, String> userDetail : userDetails) {
							userName = userDetail.getColumns().getStringValue("username", null);
						}
					} catch (Exception e) {
						LOG.info("Error while fetching User uid ");
					}
				} else if (activityRow.getStringValue("user_id", null) != null) {
					try {
						ColumnList<String> userUidList = baseDao.readWithKey(ColumnFamilySet.DIMUSER.getColumnFamily(), activityRow.getStringValue("user_id", null));
						userUid = userUidList.getStringValue(Constants._GOORU_UID, null);

						Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamilySet.DIMUSER.getColumnFamily(), Constants._GOORU_UID, activityRow.getStringValue(Constants._GOORU_UID, null));
						for (Row<String, String> userDetail : userDetails) {
							userName = userDetail.getColumns().getStringValue("username", null);
						}
					} catch (Exception e) {
						LOG.info("Error while fetching User uid ");
					}
				}
				if (userUid != null && eventId != null) {
					LOG.info("userUid {} ", userUid);
					this.updateActivityCompletion(userUid, activityRow, eventId, timeMap);
				} else {
					return;
				}

				if (rawMap != null && rawMap.get(Constants.API_KEY) != null) {
					apiKey = rawMap.get(Constants.API_KEY);
				} else if (activityRow.getStringValue(Constants.API_KEY, null) != null) {
					apiKey = activityRow.getStringValue(Constants.API_KEY, null);
				}
				if (rawMap != null && rawMap.get(Constants.CONTENT_GOORU_OID) != null) {
					contentGooruId = rawMap.get(Constants.CONTENT_GOORU_OID);
				} else if (activityRow.getStringValue(Constants._CONTENT_GOORU_OID, null) != null) {
					contentGooruId = activityRow.getStringValue(Constants._CONTENT_GOORU_OID, null);
				}
				if (rawMap != null && rawMap.get(Constants.PARENT_GOORU_OID) != null) {
					parentGooruId = rawMap.get(Constants.PARENT_GOORU_OID);
				} else if (activityRow.getStringValue(Constants._PARENT_GOORU_OID, null) != null) {
					parentGooruId = activityRow.getStringValue(Constants._PARENT_GOORU_OID, null);
				}
				if (rawMap != null && rawMap.get(Constants.ORGANIZATION_UID) != null) {
					organizationUid = rawMap.get(Constants.ORGANIZATION_UID);
				} else if (activityRow.getStringValue(Constants._ORGANIZATION_UID, null) != null) {
					organizationUid = activityRow.getStringValue(Constants._ORGANIZATION_UID, null);
				}
				activityMap.put(Constants.EVENT_ID, eventId);
				activityMap.put(Constants.EVENT_NAME, activityRow.getStringValue(Constants._EVENT_NAME, null));
				activityMap.put("userUid", userUid);
				activityMap.put("dateId", dateId);
				activityMap.put("userName", userName);
				activityMap.put("apiKey", apiKey);
				activityMap.put(Constants.ORGANIZATION_UID, organizationUid);
				activityMap.put("existingColumnName", timeMap.get("existingColumnName"));

				eventMap.put("start_time", timeMap.get(Constants.START_TIME));
				eventMap.put("end_time", timeMap.get(Constants.END_TIME));
				eventMap.put("event_type", timeMap.get("event_type"));
				eventMap.put("timeSpent", timeMap.get("timeSpent"));

				eventMap.put("user_uid", userUid);
				eventMap.put("username", userName);
				eventMap.put("raw_data", activityRow.getStringValue(Constants.FIELDS, null));
				eventMap.put("content_gooru_oid", contentGooruId);
				eventMap.put("parent_gooru_oid", parentGooruId);
				eventMap.put(Constants._ORGANIZATION_UID, organizationUid);
				eventMap.put("event_name", activityRow.getStringValue(Constants._EVENT_NAME, null));
				eventMap.put("event_value", activityRow.getStringValue(Constants._EVENT_VALUE, null));

				eventMap.put("event_id", eventId);
				eventMap.put("api_key", apiKey);
				eventMap.put(Constants._ORGANIZATION_UID, organizationUid);

				activityMap.put("activity", new JSONSerializer().serialize(eventMap));

				if (userUid != null) {
					baseDao.saveActivity(ColumnFamilySet.ACITIVITYSTREAM.getColumnFamily(), activityMap);
				}
			}
		}
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
			baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), Constants.ACTIVITY_INDEX_STATUS, Constants.DEFAULT_COLUMN, Constants.INPROGRESS);
			baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), Constants.ACTIVITY_INDEX_CHECKED_COUNT, Constants.DEFAULT_COLUMN, "" + 0);
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
				baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), Constants.ACTIVITY_INDEX_LAST_UPDATED, Constants.DEFAULT_COLUMN, "" + minuteDateFormatter.format(new Date(batchStartDate)));
			}
		}

		if (isScheduledJob) {
			baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), Constants.ACTIVITY_INDEX_STATUS, Constants.DEFAULT_COLUMN, Constants.COMPLETED);
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
	private String updateEvent(EventData eventData) {
		if (eventData.getTimeSpentInMs() != null) {
			eventData.setTimeInMillSec(eventData.getTimeSpentInMs());
		}
		return baseDao.saveEvent(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventData);
	}

	public Map<String, String> getKafkaProperty(String propertyName) {
		return DataLoggerCaches.getKafkaConfigurationCache().get(propertyName);
	}
}
