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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.ednovo.data.model.JSONDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.cassandra.loader.dao.ELSIndexerImpl;
import org.logger.event.cassandra.loader.dao.LiveDashBoardDAOImpl;
import org.logger.event.cassandra.loader.dao.MicroAggregatorDAOmpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.util.TimeUUIDUtils;

import flexjson.JSONSerializer;

public class CassandraDataLoader implements Constants {

	private static final Logger logger = LoggerFactory.getLogger(CassandraDataLoader.class);

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	private SimpleDateFormat minuteDateFormatter;

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	private CassandraConnectionProvider connectionProvider;

	private KafkaLogProducer kafkaLogWriter;

	private MicroAggregatorDAOmpl liveAggregator;

	private LiveDashBoardDAOImpl liveDashBoardDAOImpl;

	private ELSIndexerImpl indexer;

	public static Map<String, String> cache;

	private static Map<String, Map<String, String>> kafkaConfigurationCache;

	public static Map<String, Object> gooruTaxonomy;

	private MicroAggregatorProducer microAggregator;

	public Collection<String> pushingEvents;

	public Collection<String> statKeys;

	public ColumnList<String> statMetrics;

	private BaseCassandraRepoImpl baseDao;

	private boolean canRunSchduler = false;

	private boolean canRunIndexing = true;
	
	private int indexMinLoopCount = 100;
	
	/**
	 * Get Kafka properties from Environment
	 */
	public CassandraDataLoader() {
		this(null);

		// micro Aggregator producer IP
		// micro Aggregator producer IP
		String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKAIP);
		String KAFKA_AGGREGATOR_PORT = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKAPORT);
		String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKATOPIC);
		String KAFKA_AGGREGATOR_TYPE = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKAPRODUCERTYPE);

		// Log Writter producer IP
		String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKAIP);
		String KAFKA_LOG_WRITTER_PORT = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKAPORT);
		String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKATOPIC);
		String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKAPRODUCERTYPE);
		kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT, KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
		microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT, KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
	}

	public CassandraDataLoader(Map<String, String> configOptionsMap) {
		init(configOptionsMap);
		// micro Aggregator producer IP
		String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKAIP);
		String KAFKA_AGGREGATOR_PORT = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKAPORT);
		String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKATOPIC);
		String KAFKA_AGGREGATOR_TYPE = getKafkaProperty(V2KAFKAMICROPRODUCER).get(KAFKAPRODUCERTYPE);

		// Log Writter producer IP
		String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKAIP);
		String KAFKA_LOG_WRITTER_PORT = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKAPORT);
		String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKATOPIC);
		String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty(V2KAFKALOGWRITERPRODUCER).get(KAFKAPRODUCERTYPE);

		microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT, KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
		kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT, KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
	}

	public static long getTimeFromUUID(UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

	/**
	 * *
	 * 
	 * @param configOptionsMap
	 *            Initialize CoulumnFamily
	 */

	private void init(Map<String, String> configOptionsMap) {

		this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

		this.setConnectionProvider(new CassandraConnectionProvider());
		this.getConnectionProvider().init(configOptionsMap);

		this.liveAggregator = new MicroAggregatorDAOmpl(getConnectionProvider());
		this.liveDashBoardDAOImpl = new LiveDashBoardDAOImpl(getConnectionProvider());
		baseDao = new BaseCassandraRepoImpl(getConnectionProvider());
		indexer = new ELSIndexerImpl(getConnectionProvider());

		Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(), 0);
		cache = new LinkedHashMap<String, String>();
		for (Row<String, String> row : operators) {
			cache.put(row.getKey(), row.getColumns().getStringValue(AGGJSON, null));
		}
		cache.put(VIEWEVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), VIEW_EVENTS, DEFAULTCOLUMN, 0).getStringValue());
		cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ATMOENDPOINT, DEFAULTCOLUMN, 0).getStringValue());
		cache.put(VIEWUPDATEENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN, 0).getStringValue());
		cache.put(SESSIONTOKEN, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN, 0).getStringValue());
		cache.put(SEARCHINDEXAPI, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULTCOLUMN, 0).getStringValue());
		cache.put(STATFIELDS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), STATFIELDS, DEFAULTCOLUMN, 0).getStringValue());
		cache.put(BATCH_SIZE, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), BATCH_SIZE, DEFAULTCOLUMN, 0).getStringValue());
		
		ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCHSTATUS, 0);
		for (int i = 0; i < schdulersStatus.size(); i++) {
			cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
		}
		pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), DEFAULTKEY, 0).getColumnNames();
		statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), STATMETRICS, 0);
		statKeys = statMetrics.getColumnNames();

		String host = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCHHOST, 0).getStringValue(HOST, null);

		try {
			String localHost = "" + InetAddress.getLocalHost();
			logger.info("Host : " + localHost);
			if (localHost.contains(host)) {
				canRunSchduler = true;
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		String realTimeIndexing = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), REALTIMEINDEXING, 0).getStringValue(DEFAULTCOLUMN, null);
		if(realTimeIndexing.equalsIgnoreCase(STOP)){
			canRunIndexing = false;
		}
		if (kafkaConfigurationCache == null) {

			kafkaConfigurationCache = new HashMap<String, Map<String, String>>();
			String[] kafkaMessager = new String[] { V2KAFKACONSUMER, V2KAFKALOGWRITERPRODUCER, V2KAFKALOGWRITERCONSUMER, V2KAFKAMICROPRODUCER, V2KAFKAMICROCONSUMER };
			Rows<String, String> result = baseDao.readCommaKeyList(CONFIG_SETTINGS, kafkaMessager);
			for (Row<String, String> row : result) {
				Map<String, String> properties = new HashMap<String, String>();
				for (Column<String> column : row.getColumns()) {
					properties.put(column.getName(), column.getStringValue());
				}
				kafkaConfigurationCache.put(row.getKey(), properties);
			}
		}
	}

	public void clearCache() {
		cache.clear();
		Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(), 0);
		cache = new LinkedHashMap<String, String>();
		for (Row<String, String> row : operators) {
			cache.put(row.getKey(), row.getColumns().getStringValue(AGGJSON, null));
		}
		cache.put(VIEWEVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), VIEW_EVENTS, DEFAULTCOLUMN, 0).getStringValue());
		cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ATMOENDPOINT, DEFAULTCOLUMN, 0).getStringValue());
		cache.put(VIEWUPDATEENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN, 0).getStringValue());
		cache.put(SESSIONTOKEN, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN, 0).getStringValue());
		cache.put(SEARCHINDEXAPI, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULTCOLUMN, 0).getStringValue());
		cache.put(STATFIELDS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), STATFIELDS, DEFAULTCOLUMN, 0).getStringValue());
		
		pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), DEFAULTKEY, 0).getColumnNames();
		statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), STATMETRICS, 0);

		statKeys = statMetrics.getColumnNames();
		liveDashBoardDAOImpl.clearCache();
		indexer.clearCache();
		ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCHSTATUS, 0);
		for (int i = 0; i < schdulersStatus.size(); i++) {
			cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
		}

		kafkaConfigurationCache = new HashMap<String, Map<String, String>>();
		String[] kafkaMessager = new String[] { V2KAFKACONSUMER, V2KAFKALOGWRITERPRODUCER, V2KAFKALOGWRITERCONSUMER, V2KAFKAMICROPRODUCER, V2KAFKAMICROCONSUMER };
		Rows<String, String> result = baseDao.readCommaKeyList(CONFIG_SETTINGS, kafkaMessager);
		for (Row<String, String> row : result) {
			Map<String, String> properties = new HashMap<String, String>();
			for (Column<String> column : row.getColumns()) {
				properties.put(column.getName(), column.getStringValue());
			}
			kafkaConfigurationCache.put(row.getKey(), properties);
		}

		String host = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCHHOST, 0).getStringValue(HOST, null);

		try {
			String localHost = "" + InetAddress.getLocalHost();
			logger.info("Host : " + localHost);
			if (localHost.contains(host)) {
				canRunSchduler = true;
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		String realTimeIndexing = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), REALTIMEINDEXING, 0).getStringValue(DEFAULTCOLUMN, null);
		if(realTimeIndexing.equalsIgnoreCase(STOP)){
			canRunIndexing = false;
		}
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
	public void handleLogMessage(String fields, Long startTime, String userAgent, String userIp, Long endTime, String apiKey, String eventName, String gooruOId, String contentId, String query, String gooruUId, String userId, String gooruId, String type, String parentEventId, String context,
			String reactionType, String organizationUid, Long timeSpentInMs, int[] answerId, int[] attemptStatus, int[] trySequence, String requestMethod, String eventId) {
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
				existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getEventId(), 0);
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
			records.put("api_key", eventData.getApiKey() != null ? eventData.getApiKey() : DEFAULT_API_KEY);
			Collection<String> existingEventRecord = baseDao.getKey(ColumnFamily.DIMEVENTS.getColumnFamily(), records);

			if (existingEventRecord == null && existingEventRecord.isEmpty()) {
				logger.info("Please add new event in to events table ");
				return;
			}

			updateEventCompletion(eventData);

			String eventKeyUUID = updateEvent(eventData);
			if (eventKeyUUID == null) {
				return;
			}
			/**
			 * write the JSON to Log file using kafka log writer module in aysnc
			 * mode. This will store/write all data to activity log file in
			 * log/event_api_logs/activity.log
			 */
			if (eventData.getFields() != null) {
				baseDao.saveEvent(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData);
				kafkaLogWriter.sendEventLog(eventData.getFields());
				logger.info("CORE: Writing to activity log - :" + eventData.getFields().toString());
			}

			// Insert into event_timeline column family
			Date eventDateTime = new Date(eventData.getStartTime());
			String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
			if (eventData.getEventType() == null || !eventData.getEventType().equalsIgnoreCase("completed-event")) {
				eventData.setEventKeyUUID(eventKeyUUID.toString());
				String duplicatekey = eventRowKey + "~" + eventRowKey;
				baseDao.updateTimeline(ColumnFamily.EVENTTIMELINE.getColumnFamily(), eventData, eventRowKey);
			}
			try {
				updateActivityStream(eventData.getEventId());
			} catch (JSONException e) {
				logger.info("Json Exception while saving Activity Stream via old event format {}", e);
			}
		} catch (ConnectionException e) {
			logger.info("Exception while processing update for rowkey {} ", e);
		}
	}

	public void handleEventObjectMessage(EventObject eventObject) throws JSONException, ConnectionException, IOException, GeoIp2Exception {
		Map<String, Object> eventMap = new LinkedHashMap<String, Object>();
		Map<String, String> eventMap2 = new LinkedHashMap<String, String>();

		String aggregatorJson = null;

		try {
			eventMap = JSONDeserializer.deserializeEventObjectv2(eventObject);
			eventMap2 = JSONDeserializer.deserializeEventObject(eventObject);
			if (eventObject.getFields() != null) {
				//logger.info("CORE: Writing to activity log - :" + eventObject.getFields().toString());
				kafkaLogWriter.sendEventLog(eventObject.getFields());

			}

			eventMap = (Map<String, Object>) this.formatEventObjectMap(eventObject, eventMap);
			eventMap2 = this.formatEventMap(eventObject, eventMap2);
			String apiKey = eventObject.getApiKey() != null ? eventObject.getApiKey() : DEFAULT_API_KEY;

			Map<String, Object> records = new HashMap<String, Object>();
			records.put(EVENT_NAME, eventMap.get(EVENTNAME));
			records.put(API_KEY, apiKey);
			Collection<String> eventId = baseDao.getKey(ColumnFamily.DIMEVENTS.getColumnFamily(), records);

			if (eventId == null || eventId.isEmpty()) {
				UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
				records.put(EVENT_ID, uuid.toString());
				String key = apiKey + SEPERATOR + uuid.toString();
				baseDao.saveBulkList(ColumnFamily.DIMEVENTS.getColumnFamily(), key, records);
			}

			updateEventObjectCompletion(eventObject);

			String eventKeyUUID = baseDao.saveEventObject(ColumnFamily.EVENTDETAIL.getColumnFamily(), null, eventObject);

			if (eventKeyUUID == null) {
				return;
			}

			Date eventDateTime = new Date(eventObject.getEndTime());
			String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();

			baseDao.updateTimelineObject(ColumnFamily.EVENTTIMELINE.getColumnFamily(), eventRowKey, eventKeyUUID.toString(), eventObject);

			aggregatorJson = cache.get(eventMap.get("eventName").toString());

			if (aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)) {
				liveAggregator.realTimeMetrics(eventMap2, aggregatorJson);
			}

			liveDashBoardDAOImpl.callCountersV2(eventMap);

			if (eventObject.getFields() != null) {
				microAggregator.sendEventForAggregation(eventObject.getFields());
			}

			if (aggregatorJson != null && !aggregatorJson.isEmpty() && aggregatorJson.equalsIgnoreCase(RAWUPDATE)) {
				liveAggregator.updateRawData(eventMap);
			}

		} catch (Exception e) {
			kafkaLogWriter.sendErrorEventLog(eventObject.getFields());
			logger.error("Writing error log : {} ", eventObject.getEventId());
		}
		if(canRunIndexing){
			indexer.indexEvents(eventObject.getFields());
		}
		
		try {

			if (cache.get(VIEWEVENTS).contains(eventMap.get("eventName").toString())) {
				liveDashBoardDAOImpl.addContentForPostViews(eventMap);
			}

			/*
			 * To be Re-enable
			 * 
			 * 
			 * liveDashBoardDAOImpl.findDifferenceInCount(eventMap);
			 * 
			 * liveDashBoardDAOImpl.addApplicationSession(eventMap);
			 * 
			 * liveDashBoardDAOImpl.saveGeoLocations(eventMap);
			 * 
			 * 
			 * if(pushingEvents.contains(eventMap.get("eventName"))){
			 * liveDashBoardDAOImpl
			 * .pushEventForAtmosphere(cache.get(ATMOSPHERENDPOINT),eventMap); }
			 * 
			 * if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CRPV1
			 * .getName())){
			 * liveDashBoardDAOImpl.pushEventForAtmosphereProgress(
			 * atmosphereEndPoint, eventMap); }
			 */

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * @param eventData
	 *            Update the event is completion status
	 * @throws ConnectionException
	 *             If the host is unavailable
	 */

	@Async
	private void updateEventObjectCompletion(EventObject eventObject) throws ConnectionException {

		Long endTime = eventObject.getEndTime(), startTime = eventObject.getStartTime();
		long timeInMillisecs = 0L;
		if (endTime != null && startTime != null) {
			timeInMillisecs = endTime - startTime;
		}
		boolean eventComplete = false;

		eventObject.setTimeInMillSec(timeInMillisecs);

		if (StringUtils.isEmpty(eventObject.getEventId())) {
			return;
		}

		ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventObject.getEventId(), 0);
		if (existingRecord != null && !existingRecord.isEmpty()) {
			if ("stop".equalsIgnoreCase(eventObject.getEventType())) {
				startTime = existingRecord.getLongValue("start_time", null);
				// Update startTime with existingRecord, IF
				// existingRecord.startTime < startTime
			} else {
				endTime = existingRecord.getLongValue("end_time", null);
				// Update endTime with existing record IF existingRecord.endTime
				// > endTime
			}
			eventComplete = true;
		}
		// Time taken for the event in milliseconds derived from the start /
		// stop events.
		if (endTime != null && startTime != null) {
			timeInMillisecs = endTime - startTime;
		}
		if (timeInMillisecs > 1147483647) {
			// When time in Milliseconds is very very huge, set to min time to
			// serve the call.
			timeInMillisecs = 30;
			// Since this is an error condition, log it.
		}

		eventObject.setStartTime(startTime);
		eventObject.setEndTime(endTime);

		if (eventComplete) {
			eventObject.setTimeInMillSec(timeInMillisecs);
			eventObject.setEventType("completed-event");
			eventObject.setEndTime(endTime);
			eventObject.setStartTime(startTime);
		}

		if (!StringUtils.isEmpty(eventObject.getParentEventId())) {
			ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventObject.getParentEventId(), 0);
			if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
				Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
				baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventObject.getParentEventId(), "end_time", endTime);
				baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventObject.getParentEventId(), "time_spent_in_millis", (endTime - parentStartTime));
			}
		}

	}

	public void callAPIViewCount() throws Exception {
		if (cache.get("stat_job").equalsIgnoreCase("stop")) {
			logger.info("job stopped");
			return;
		}
		String lastUpadatedTime = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), VIEWSLASTUPDATED, DEFAULTCOLUMN, 0).getStringValue();
		String currentTime = minuteDateFormatter.format(new Date()).toString();
		Date lastDate = null;
		Date currDate = null;

		try {
			lastDate = minuteDateFormatter.parse(lastUpadatedTime);
			currDate = minuteDateFormatter.parse(currentTime);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		Date rowValues = new Date(lastDate.getTime() + 60000);

		logger.info("1-processing mins : {} ,current mins :{} ", minuteDateFormatter.format(rowValues), minuteDateFormatter.format(currDate));

		if ((rowValues.getTime() < currDate.getTime())) {
			ColumnList<String> contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS + SEPERATOR + minuteDateFormatter.format(rowValues), 0);
			ColumnList<String> indexedCountList = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS + SEPERATOR + INDEXEDLIMIT, 0);
			int indexedCount = indexedCountList != null ? Integer.valueOf(indexedCountList.getStringValue(minuteDateFormatter.format(rowValues), "0")) : 0;

			boolean status = this.getRecordsToProcess(rowValues, INDEXEDLIMIT);

			if ((contents.size() == 0 || indexedCount == (contents.size() - 1)) && status) {
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), VIEWSLASTUPDATED, DEFAULTCOLUMN, minuteDateFormatter.format(rowValues));
			}
		}

		/*
		 * 
		 * Disabling this code to avoid hitting search index performance more
		 * 
		 * 
		 * logger.info("2-processing curr mins : {}",minuteDateFormatter.format(
		 * currDate)); boolean status =
		 * this.getRecordsToProcess(currDate,CURRINDEXINGLIMIT);
		 */

	}

	private boolean getRecordsToProcess(Date rowValues, String indexLabelLimit) throws Exception {

		String indexCollectionType = null;
		String indexResourceType = null;
		String IndexingStatus = null;
		String resourceIds = "";
		String collectionIds = "";
		int indexedCount = 0;
		int indexedLimit = 2;
		int allowedLimit = 0;
		MutationBatch m2 = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		ColumnList<String> contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS + SEPERATOR + minuteDateFormatter.format(rowValues), 0);
		ColumnList<String> indexedCountList = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS + SEPERATOR + indexLabelLimit, 0);
		indexedCount = indexedCountList != null ? Integer.valueOf(indexedCountList.getStringValue(minuteDateFormatter.format(rowValues), "0")) : 0;

		logger.info("1:-> size : " + contents.size() + "indexed count : " + indexedCount);

		if (contents.size() > 0) {
			ColumnList<String> IndexLimitList = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXLIMIT, 0);
			indexedLimit = IndexLimitList != null ? Integer.valueOf(IndexLimitList.getStringValue(DEFAULTCOLUMN, "0")) : 2;
			allowedLimit = (indexedCount + indexedLimit);
			if (allowedLimit > contents.size()) {
				allowedLimit = indexedCount + (contents.size() - indexedCount);
			}
			ColumnList<String> indexingStat = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXSTATUS, 0);
			IndexingStatus = indexingStat.getStringValue(DEFAULTCOLUMN, null);
			if (IndexingStatus.equalsIgnoreCase(COMPLETED)) {
				for (int i = indexedCount; i < allowedLimit; i++) {
					indexedCount = i;
					ColumnList<String> vluesList = baseDao.readWithKeyColumnList(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~" + contents.getColumnByIndex(i).getStringValue(), statKeys, 0);
					for (Column<String> detail : vluesList) {
						JSONObject resourceObj = new JSONObject();
						resourceObj.put("gooruOid", contents.getColumnByIndex(i).getStringValue());
						ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.RESOURCE.getColumnFamily(), contents.getColumnByIndex(i).getStringValue(), 0);
						;
						if (resource.getColumnByName("resourceType") != null && resource.getColumnByName("resourceType").getStringValue().equalsIgnoreCase("scollection")) {
							indexCollectionType = "scollection";
							if (!collectionIds.contains(contents.getColumnByIndex(i).getStringValue())) {
								collectionIds += "," + contents.getColumnByIndex(i).getStringValue();
							}
						} else {
							indexResourceType = "resource";
							if (!resourceIds.contains(contents.getColumnByIndex(i).getStringValue())) {
								resourceIds += "," + contents.getColumnByIndex(i).getStringValue();
							}
						}
						for (String column : statKeys) {
							if (detail.getName().equals(column)) {
								if (statMetrics.getStringValue(column, null) != null) {
									baseDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(), contents.getColumnByIndex(i).getStringValue(), statMetrics.getStringValue(column, null), detail.getLongValue(), m2);
									if (statMetrics.getStringValue(column, null).equalsIgnoreCase("statistics.totalTimeSpent") && vluesList.getLongValue("count~views", 0L) != 0L) {
										baseDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(), contents.getColumnByIndex(i).getStringValue(), "statistics.averageTimeSpent", (detail.getLongValue() / vluesList.getLongValue("count~views", 0L)), m2);
									}
									if (statMetrics.getStringValue(column, null).equalsIgnoreCase("statistics.viewsCountN")) {
										baseDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(), contents.getColumnByIndex(i).getStringValue(), "statistics.viewsCount", "" + detail.getLongValue(), m2);
									}
								}
							}
						}

					}
				}
			}
			if (indexCollectionType != null || indexResourceType != null) {
				// m.execute();
				m2.execute();
				int indexingStatus = 0;
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXSTATUS, DEFAULTCOLUMN, INPROGRESS);
				if (indexCollectionType != null) {
					indexingStatus = this.callStatAPI(indexCollectionType, collectionIds.substring(1));
				}
				if (indexResourceType != null) {
					indexingStatus = this.callStatAPI(indexResourceType, resourceIds.substring(1));
				}
				baseDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS + SEPERATOR + indexLabelLimit, minuteDateFormatter.format(rowValues), String.valueOf(indexedCount++), 86400);

				if (indexingStatus == 200 || indexingStatus == 404) {
					baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXSTATUS, DEFAULTCOLUMN, COMPLETED);
					baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXWAITINGCOUNT, DEFAULTCOLUMN, "0");
					return true;
				} else {
					logger.info("Statistical data update failed");
					return false;
				}
			} else if (IndexingStatus != null && IndexingStatus.equalsIgnoreCase(INPROGRESS)) {
				ColumnList<String> indexWaitingLimit = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXWAITINGLIMIT, 0);
				String limit = indexWaitingLimit != null ? indexWaitingLimit.getStringValue(DEFAULTCOLUMN, "0") : "0";

				ColumnList<String> indexWaitingCount = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXWAITINGCOUNT, 0);
				String count = indexWaitingCount != null ? indexWaitingCount.getStringValue(DEFAULTCOLUMN, "0") : "0";

				if (Integer.valueOf(count) > Integer.valueOf(limit)) {
					baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXSTATUS, DEFAULTCOLUMN, COMPLETED);
					baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXWAITINGCOUNT, DEFAULTCOLUMN, "0");
				} else {
					baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXWAITINGCOUNT, DEFAULTCOLUMN, "" + (Integer.valueOf(count) + 1));
				}
				logger.info("Waiting for indexing" + (Integer.valueOf(count) + 1));
				return false;
			}
		} else {
			logger.info("No content is viewed");
			return true;
		}
		return false;
	}

	private int callStatAPI(String resourceType, String ids) {

		try {
			String url = cache.get(VIEWUPDATEENDPOINT) + resourceType + "/reindex-stas?sessionToken=" + cache.get(SESSIONTOKEN) + "&indexableIds=" + ids + "&fields="+cache.get(STATFIELDS);
			DefaultHttpClient httpClient = new DefaultHttpClient();
			HttpPost postRequest = new HttpPost(url);
			logger.info("Indexing url : {} ", url);
			HttpResponse response = httpClient.execute(postRequest);
			logger.info("Status : {} ", response.getStatusLine().getStatusCode());
			logger.info("Reason : {} ", response.getStatusLine().getReasonPhrase());
			if (response.getStatusLine().getStatusCode() != 200) {
				logger.info("Stat Indexing failed...");
				return response.getStatusLine().getStatusCode();
			} else {
				logger.info("Stat Indexing call Success...");
				return response.getStatusLine().getStatusCode();
			}
		} catch (Exception e) {
			logger.error("Stat Indexing failed..." + e);
			return 500;
		}
	}
	
	private int callIndexingAPI(String resourceType, String ids) {

		try {
			String sessionToken = cache.get(SESSIONTOKEN);
			String url = cache.get(SEARCHINDEXAPI) + resourceType + "/index?sessionToken=" + sessionToken + "&ids=" + ids;
			DefaultHttpClient httpClient = new DefaultHttpClient();
			HttpPost postRequest = new HttpPost(url);
			logger.info("Indexing url : {} ", url);
			HttpResponse response = httpClient.execute(postRequest);
			logger.info("Status : {} ", response.getStatusLine().getStatusCode());
			logger.info("Reason : {} ", response.getStatusLine().getReasonPhrase());
			if (response.getStatusLine().getStatusCode() != 200) {
				logger.info("Search Indexing failed...");
				return response.getStatusLine().getStatusCode();
			} else {
				logger.info("Search Indexing call Success...");
				return response.getStatusLine().getStatusCode();
			}
		} catch (Exception e) {
			logger.info("Search Indexing failed..." + e);
			return 500;
		}
	}

	public void updateActivityCompletion(String userUid, ColumnList<String> activityRow, String eventId, Map<String, Object> timeMap) {
		Long startTime = activityRow.getLongValue(START_TIME, 0L), endTime = activityRow.getLongValue(END_TIME, 0L);
		String eventType = null;
		JsonElement jsonElement = null;
		JsonObject existingEventObj = null;
		String existingColumnName = null;

		if (activityRow.getStringValue(EVENT_TYPE, null) != null) {
			eventType = activityRow.getStringValue(EVENT_TYPE, null);
		}

		long timeInMillisecs = 0L;
		if (endTime != null && startTime != null) {
			timeInMillisecs = endTime - startTime;
		}

		if (!StringUtils.isEmpty(eventType) && userUid != null) {
			Map<String, Object> existingRecord = baseDao.isEventIdExists(ColumnFamily.ACITIVITYSTREAM.getColumnFamily(), userUid, eventId);
			if (existingRecord.get("isExists").equals(true) && existingRecord.get("jsonString").toString() != null) {
				jsonElement = new JsonParser().parse(existingRecord.get("jsonString").toString());
				existingEventObj = jsonElement.getAsJsonObject();
				if ("completed-event".equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType)) {
					existingColumnName = existingRecord.get("existingColumnName").toString();
					startTime = existingEventObj.get(START_TIME).getAsLong();
				} else {
					endTime = existingEventObj.get(END_TIME).getAsLong();
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
		timeMap.put("startTime", startTime);
		timeMap.put("endTime", endTime);
		timeMap.put("event_type", eventType);
		timeMap.put("existingColumnName", existingColumnName);
		timeMap.put("timeSpent", timeInMillisecs);

	}

	/**
	 * @return the connectionProvider
	 */
	public CassandraConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	public void executeForEveryMinute(String startTime, String endTime) {
		logger.debug("start the static loader");
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("startTime", startTime);
			jsonObject.put("endTime", endTime);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		microAggregator.sendEventForStaticAggregation(jsonObject.toString());
	}

	public void watchSession() {
		try {
			liveDashBoardDAOImpl.watchApplicationSession();
		} catch (ParseException e) {
			logger.info("Exception : {} ", e);
		}
	}

	public boolean createEvent(String eventName, String apiKey) {

		Map<String, Object> records = new HashMap<String, Object>();
		apiKey = apiKey != null ? apiKey : DEFAULT_API_KEY;
		records.put("api_key", apiKey);
		records.put("event_name", eventName);
		if (baseDao.isValueExists(ColumnFamily.DIMEVENTS.getColumnFamily(), records)) {

			UUID eventId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			records.put("event_id", eventId.toString());
			String key = apiKey + SEPERATOR + eventId.toString();
			baseDao.saveBulkList(ColumnFamily.DIMEVENTS.getColumnFamily(), key, records);
			return true;
		}
		return false;
	}

	public boolean validateSchedular() {

		return canRunSchduler;
	}

	private Map<String, String> formatEventMap(EventObject eventObject, Map<String, String> eventMap) {

		String userUid = null;
		String organizationUid = DEFAULT_ORGANIZATION_UID;
		eventObject.setParentGooruId(eventMap.get("parentGooruId"));
		eventObject.setContentGooruId(eventMap.get("contentGooruId"));
		if (eventMap.containsKey("parentEventId") && eventMap.get("parentEventId") != null) {
			eventObject.setParentEventId(eventMap.get("parentEventId"));
		}
		eventObject.setTimeInMillSec(Long.parseLong(eventMap.get("totalTimeSpentInMs")));
		eventObject.setEventType(eventMap.get("type"));

		if (eventMap != null && eventMap.get("gooruUId") != null && eventMap.containsKey("organizationUId") && (eventMap.get("organizationUId") == null || eventMap.get("organizationUId").isEmpty())) {
			try {
				userUid = eventMap.get("gooruUId");
				Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid, 0);
				for (Row<String, String> userDetail : userDetails) {
					organizationUid = userDetail.getColumns().getStringValue("organization_uid", null);
				}
				eventObject.setOrganizationUid(organizationUid);
				JSONObject sessionObj = new JSONObject(eventObject.getSession());
				sessionObj.put("organizationUId", organizationUid);
				eventObject.setSession(sessionObj.toString());
				JSONObject fieldsObj = new JSONObject(eventObject.getFields());
				fieldsObj.put("session", sessionObj.toString());
				eventObject.setFields(fieldsObj.toString());
				eventMap.put("organizationUId", organizationUid);
			} catch (Exception e) {
				logger.info("Error while fetching User uid ");
			}
		}

		eventMap.put("eventName", eventObject.getEventName());
		eventMap.put("eventId", eventObject.getEventId());
		eventMap.put("startTime", String.valueOf(eventObject.getStartTime()));
		eventMap.put("endTime", String.valueOf(eventObject.getEndTime()));
		return eventMap;
	}

	private <T> T formatEventObjectMap(EventObject eventObject, Map<String, T> eventMap) {

		String userUid = null;
		String organizationUid = DEFAULT_ORGANIZATION_UID;
		if (eventMap.containsKey("parentGooruId") && eventMap.get("parentGooruId") != null) {
			eventObject.setParentGooruId(eventMap.get("parentGooruId").toString());
		}
		if (eventMap.containsKey("contentGooruId") && eventMap.get("contentGooruId") != null) {
			eventObject.setContentGooruId(eventMap.get("contentGooruId").toString());
		}
		if (eventMap.containsKey("parentEventId") && eventMap.get("parentEventId") != null) {
			eventObject.setParentEventId(eventMap.get("parentEventId").toString());
		}
		eventObject.setTimeInMillSec(Long.parseLong(eventMap.get("totalTimeSpentInMs").toString()));
		if (eventMap.containsKey("type") && eventMap.get("type") != null) {
			eventObject.setEventType(eventMap.get("type").toString());
		}

		if (eventMap != null && eventMap.get("gooruUId") != null && eventMap.containsKey("organizationUId") && (eventMap.get("organizationUId") == null || eventMap.get("organizationUId").toString().isEmpty())) {
			try {
				userUid = eventMap.get("gooruUId").toString();
				Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid, 0);
				for (Row<String, String> userDetail : userDetails) {
					organizationUid = userDetail.getColumns().getStringValue("organization_uid", null);
				}
				eventObject.setOrganizationUid(organizationUid);
				JSONObject sessionObj = new JSONObject(eventObject.getSession());
				sessionObj.put("organizationUId", organizationUid);
				eventObject.setSession(sessionObj.toString());
				JSONObject fieldsObj = new JSONObject(eventObject.getFields());
				fieldsObj.put("session", sessionObj.toString());
				eventObject.setFields(fieldsObj.toString());
				eventMap.put("organizationUId", (T) organizationUid);
			} catch (Exception e) {
				logger.info("Error while fetching User uid ");
			}
		}
		eventMap.put("eventName", (T) eventObject.getEventName());
		eventMap.put("eventId", (T) eventObject.getEventId());
		eventMap.put("startTime", (T) String.valueOf(eventObject.getStartTime()));

		return (T) eventMap;
	}

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

	@Async
	public void migrateCF(String cfName) {
		Rows<String, String> cfData = baseDao.readAllRows(cfName, 0);

		for (Row<String, String> row : cfData) {
			MutationBatch m = null;
			try {
				m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
				String Key = row.getKey();
				logger.info("Key" + Key);
				for (Column<String> columns : row.getColumns()) {
					baseDao.generateNonCounter(cfName, Key, columns.getName(), columns.getStringValue(), m);
				}
				m.execute();
			} catch (Exception e) {
				e.printStackTrace();
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
			ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getEventId(), 0);
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
			eventData.setEventType("completed-event");
			eventData.setEndTime(endTime);
			eventData.setStartTime(startTime);
		}

		if (!StringUtils.isEmpty(eventData.getParentEventId())) {
			ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), 0);
			if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
				Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
				baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), "end_time", endTime);
				baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), "time_spent_in_millis", (endTime - parentStartTime));

			}
		}

	}

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

			ColumnList<String> activityRow = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventId, 0);
			if (activityRow != null) {
				String fields = activityRow.getStringValue(FIELDS, null);

				SimpleDateFormat minuteDateFormatter = new SimpleDateFormat(MINUTEDATEFORMATTER);
				HashMap<String, Object> activityMap = new HashMap<String, Object>();
				Map<String, Object> eventMap = new HashMap<String, Object>();
				if (activityRow.getLongValue(END_TIME, null) != null) {
					endDate = new Date(activityRow.getLongValue(END_TIME, null));
				} else {
					endDate = new Date(activityRow.getLongValue(START_TIME, null));
				}
				dateId = minuteDateFormatter.format(endDate).toString();
				Map<String, Object> timeMap = new HashMap<String, Object>();

				// Get userUid
				if (rawMap != null && rawMap.get("gooruUId") != null) {
					try {
						userUid = rawMap.get("gooruUId");
						Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid, 0);
						for (Row<String, String> userDetail : userDetails) {
							userName = userDetail.getColumns().getStringValue("username", null);
						}
					} catch (Exception e) {
						logger.info("Error while fetching User uid ");
					}
				} else if (activityRow.getStringValue("gooru_uid", null) != null) {
					try {
						userUid = activityRow.getStringValue("gooru_uid", null);
						Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", activityRow.getStringValue("gooru_uid", null), 0);
						for (Row<String, String> userDetail : userDetails) {
							userName = userDetail.getColumns().getStringValue("username", null);
						}
					} catch (Exception e) {
						logger.info("Error while fetching User uid ");
					}
				} else if (activityRow.getStringValue("user_id", null) != null) {
					try {
						ColumnList<String> userUidList = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), activityRow.getStringValue("user_id", null), 0);
						userUid = userUidList.getStringValue("gooru_uid", null);

						Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", activityRow.getStringValue("gooru_uid", null), 0);
						for (Row<String, String> userDetail : userDetails) {
							userName = userDetail.getColumns().getStringValue("username", null);
						}
					} catch (Exception e) {
						logger.info("Error while fetching User uid ");
					}
				}
				if (userUid != null && eventId != null) {
					logger.info("userUid {} ", userUid);
					this.updateActivityCompletion(userUid, activityRow, eventId, timeMap);
				} else {
					return;
				}

				if (rawMap != null && rawMap.get(APIKEY) != null) {
					apiKey = rawMap.get(APIKEY);
				} else if (activityRow.getStringValue(APIKEY, null) != null) {
					apiKey = activityRow.getStringValue(APIKEY, null);
				}
				if (rawMap != null && rawMap.get(CONTENTGOORUOID) != null) {
					contentGooruId = rawMap.get(CONTENTGOORUOID);
				} else if (activityRow.getStringValue(CONTENT_GOORU_OID, null) != null) {
					contentGooruId = activityRow.getStringValue(CONTENT_GOORU_OID, null);
				}
				if (rawMap != null && rawMap.get(PARENTGOORUOID) != null) {
					parentGooruId = rawMap.get(PARENTGOORUOID);
				} else if (activityRow.getStringValue(PARENT_GOORU_OID, null) != null) {
					parentGooruId = activityRow.getStringValue(PARENT_GOORU_OID, null);
				}
				if (rawMap != null && rawMap.get(ORGANIZATIONUID) != null) {
					organizationUid = rawMap.get(ORGANIZATIONUID);
				} else if (activityRow.getStringValue("organization_uid", null) != null) {
					organizationUid = activityRow.getStringValue("organization_uid", null);
				}
				activityMap.put("eventId", eventId);
				activityMap.put("eventName", activityRow.getStringValue(EVENT_NAME, null));
				activityMap.put("userUid", userUid);
				activityMap.put("dateId", dateId);
				activityMap.put("userName", userName);
				activityMap.put("apiKey", apiKey);
				activityMap.put("organizationUid", organizationUid);
				activityMap.put("existingColumnName", timeMap.get("existingColumnName"));

				eventMap.put("start_time", timeMap.get("startTime"));
				eventMap.put("end_time", timeMap.get("endTime"));
				eventMap.put("event_type", timeMap.get("event_type"));
				eventMap.put("timeSpent", timeMap.get("timeSpent"));

				eventMap.put("user_uid", userUid);
				eventMap.put("username", userName);
				eventMap.put("raw_data", activityRow.getStringValue(FIELDS, null));
				eventMap.put("content_gooru_oid", contentGooruId);
				eventMap.put("parent_gooru_oid", parentGooruId);
				eventMap.put("organization_uid", organizationUid);
				eventMap.put("event_name", activityRow.getStringValue(EVENT_NAME, null));
				eventMap.put("event_value", activityRow.getStringValue(EVENT_VALUE, null));

				eventMap.put("event_id", eventId);
				eventMap.put("api_key", apiKey);
				eventMap.put("organization_uid", organizationUid);

				activityMap.put("activity", new JSONSerializer().serialize(eventMap));

				if (userUid != null) {
					baseDao.saveActivity(ColumnFamily.ACITIVITYSTREAM.getColumnFamily(), activityMap);
				}
			}
		}
	}


	/**
	 * Index all events between a given start and end times. 
	 * @param startTime
	 * @param endTime
	 * @param customEventName
	 * @param isScheduledJob true if called from scheduler. false when called from real-time indexing.
	 * @throws ParseException
	 */
	public void updateStagingES(String startTime, String endTime, String customEventName, boolean isScheduledJob) throws ParseException {

		if (isScheduledJob) {
			// Called from Scheduled job. Mark the status as in-progress so that no other activity will overlap.
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITYINDEXSTATUS, DEFAULTCOLUMN, INPROGRESS);
		}

		String timeLineKey = null;
		final long batchEndDate = minuteDateFormatter.parse(endTime).getTime();
		
		for (long batchStartDate = minuteDateFormatter.parse(startTime).getTime(); batchStartDate < batchEndDate;) {
			String currentDate = minuteDateFormatter.format(new Date(batchStartDate));
			logger.info("Processing Date : {}", currentDate);
			if (StringUtils.isBlank(customEventName)) {
				timeLineKey = currentDate.toString();
			} else {
				timeLineKey = currentDate.toString() + "~" + customEventName;
			}

			// Read Event Time Line for event keys and create as a Collection
			ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey, 0);

			if (eventUUID != null && !eventUUID.isEmpty()) {
				for (Column<String> column : eventUUID) {
					logger.debug("eventDetailUUID  : " + column.getStringValue());
					ColumnList<String> event = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), column.getStringValue(), 0);
					indexer.indexEvents(event.getStringValue("fields", null));
				}
				
			}
			// Incrementing time - one minute
			batchStartDate = new Date(batchStartDate).getTime() + 60000;

			if (isScheduledJob) {
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITYINDEXLASTUPDATED, DEFAULTCOLUMN, "" + minuteDateFormatter.format(new Date(batchStartDate)));
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITYINDEXCHECKEDCOUNT, DEFAULTCOLUMN, "" + 0);
			}
		}

		if (isScheduledJob) {
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITYINDEXSTATUS, DEFAULTCOLUMN, COMPLETED);
		}

		logger.info("Indexing completed..........");
	}

    
	@Async
	private String updateEvent(EventData eventData) {
		ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(), eventData.getApiKey(), 0);
		String appOid = apiKeyValues.getStringValue("app_oid", null);
		if (eventData.getTimeSpentInMs() != null) {
			eventData.setTimeInMillSec(eventData.getTimeSpentInMs());
		}
		return baseDao.saveEvent(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData);
	}

	/**
	 * @param connectionProvider
	 *            the connectionProvider to set
	 */
	public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
	}

	public Map<String, String> getKafkaProperty(String propertyName) {
		return kafkaConfigurationCache.get(propertyName);
	}
}
