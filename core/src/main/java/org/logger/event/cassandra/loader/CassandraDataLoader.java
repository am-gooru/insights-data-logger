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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.ednovo.data.geo.location.GeoLocation;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.ednovo.data.model.JSONDeserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.cassandra.loader.dao.LiveDashBoardDAOImpl;
import org.logger.event.cassandra.loader.dao.MicroAggregatorDAOmpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.access.AccessDeniedException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.netflix.astyanax.Keyspace;
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
    
    private Keyspace cassandraKeyspace;
    
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
    
    private SimpleDateFormat minuteDateFormatter;
    
    private SimpleDateFormat dateFormatter;
    
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    
    private CassandraConnectionProvider connectionProvider;
    
    private KafkaLogProducer kafkaLogWriter;
  
    private MicroAggregatorDAOmpl liveAggregator;
    
    private LiveDashBoardDAOImpl liveDashBoardDAOImpl;
    
    public static  Map<String,String> cache;
    
    private MicroAggregatorProducer microAggregator;
    
    private static GeoLocation geo;
    
    public Collection<String> pushingEvents ;
    
    public Collection<String> statKeys ;
    
    public ColumnList<String> statMetrics ;
        
    private BaseCassandraRepoImpl baseDao ;
    
    /**
     * Get Kafka properties from Environment
     */
    public CassandraDataLoader() {
        this(null);
        
        //micro Aggregator producer IP
        String KAFKA_AGGREGATOR_PRODUCER_IP = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_PRODUCER_IP");
        //Log Writter producer IP
        String KAFKA_LOG_WRITTER_PRODUCER_IP = System.getenv("INSIGHTS_KAFKA_LOG_WRITTER_PRODUCER_IP");
        String KAFKA_PORT = System.getenv("INSIGHTS_KAFKA_PORT");
        String KAFKA_ZK_PORT = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
        String KAFKA_TOPIC = System.getenv("INSIGHTS_KAFKA_TOPIC");
        String KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
        String KAFKA_AGGREGATOR_TOPIC = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_TOPIC");
        String KAFKA_PRODUCER_TYPE = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
        
        kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_ZK_PORT,  KAFKA_FILE_TOPIC, KAFKA_PRODUCER_TYPE);
        microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_ZK_PORT,  KAFKA_AGGREGATOR_TOPIC, KAFKA_PRODUCER_TYPE);
    }

    public CassandraDataLoader(Map<String, String> configOptionsMap) {
        init(configOptionsMap);
        //micro Aggregator producer IP
        String KAFKA_AGGREGATOR_PRODUCER_IP = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_PRODUCER_IP");
        //Log Writter producer IP
        String KAFKA_LOG_WRITTER_PRODUCER_IP = System.getenv("INSIGHTS_KAFKA_LOG_WRITTER_PRODUCER_IP");
        String KAFKA_PORT = System.getenv("INSIGHTS_KAFKA_PORT");
        String KAFKA_ZK_PORT = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
        String KAFKA_TOPIC = System.getenv("INSIGHTS_KAFKA_TOPIC");
        String KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
        String KAFKA_AGGREGATOR_TOPIC = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_TOPIC");
        String KAFKA_PRODUCER_TYPE = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
        
        microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_ZK_PORT,  KAFKA_AGGREGATOR_TOPIC, KAFKA_PRODUCER_TYPE);
        kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_ZK_PORT,  KAFKA_FILE_TOPIC, KAFKA_PRODUCER_TYPE);
    }

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    /**
     * *
     * @param configOptionsMap
     * Initialize CoulumnFamily
     */
    
    private void init(Map<String, String> configOptionsMap) {
    	
        this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
        this.dateFormatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        
        this.setConnectionProvider(new CassandraConnectionProvider());
        this.getConnectionProvider().init(configOptionsMap);

        this.liveAggregator = new MicroAggregatorDAOmpl(getConnectionProvider());
        this.liveDashBoardDAOImpl = new LiveDashBoardDAOImpl(getConnectionProvider());
        baseDao = new BaseCassandraRepoImpl(getConnectionProvider());

        Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily());
        cache = new LinkedHashMap<String, String>();
        for (Row<String, String> row : operators) {
        	cache.put(row.getKey(), row.getColumns().getStringValue("aggregator_json", null));
		}
        cache.put(VIEWEVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~events", DEFAULTCOLUMN).getStringValue());
        cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "atmosphere.end.point", DEFAULTCOLUMN).getStringValue());
        cache.put(VIEWUPDATEENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN).getStringValue());

        geo = new GeoLocation();
        
        ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "schdulers~status");
        for(int i = 0 ; i < schdulersStatus.size() ; i++) {
        	cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
        }
        pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "default~key").getColumnNames();
        statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat~metrics");
        statKeys = statMetrics.getColumnNames();
        
    }

    public void clearCache(){
    	cache.clear();
    	Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily());
        cache = new LinkedHashMap<String, String>();
        for (Row<String, String> row : operators) {
        	cache.put(row.getKey(), row.getColumns().getStringValue("aggregator_json", null));
		}
        cache.put(VIEWEVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~events", DEFAULTCOLUMN).getStringValue());
        cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "atmosphere.end.point", DEFAULTCOLUMN).getStringValue());
        cache.put(VIEWUPDATEENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN).getStringValue());
        pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "default~key").getColumnNames();
        statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat~metrics");
        statKeys = statMetrics.getColumnNames();
        liveDashBoardDAOImpl.clearCache();
        ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "schdulers~status");
        for(int i = 0 ; i < schdulersStatus.size() ; i++) {
        	cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
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
     * Generate EventData Object 
     */
    public void handleLogMessage(String fields, Long startTime,
            String userAgent, String userIp, Long endTime, String apiKey,
            String eventName, String gooruOId, String contentId, String query,String gooruUId,String userId,String gooruId,String type,
            String parentEventId,String context,String reactionType,String organizationUid,Long timeSpentInMs,int[] answerId,int[] attemptStatus,int[] trySequence,String requestMethod, String eventId) {
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
     * 		process EventData Object 
     * @exception ConnectionException
     * 		If the host is unavailable
     * 
     */
    public void handleLogMessage(EventData eventData) {
    	
    	// Increment Resource view counts for real time
    	
    	this.getAndSetAnswerStatus(eventData);
    	    	
    	if(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CR.getName())){
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
	        	 existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getEventId());
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
	         Map<String,Object> records = new HashMap<String, Object>();
	         records.put("event_name", eventData.getEventName());
	         records.put("api_key",eventData.getApiKey() != null ? eventData.getApiKey() : DEFAULT_API_KEY );
	         Collection<String> existingEventRecord = baseDao.getKey(ColumnFamily.DIMEVENTS.getColumnFamily(),records);
	
	         if(existingEventRecord == null && existingEventRecord.isEmpty()){
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
			 * mode. This will store/write all data to activity log file in log/event_api_logs/activity.log
			 */
			if (eventData.getFields() != null) {
				baseDao.saveEvent(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData);
				kafkaLogWriter.sendEventLog(eventData.getFields());
				logger.info("CORE: Writing to activity log - :"+ eventData.getFields().toString());
			}
	    
	
	        // Insert into event_timeline column family
	        Date eventDateTime = new Date(eventData.getStartTime());
	        String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
	        if(eventData.getEventType() == null || !eventData.getEventType().equalsIgnoreCase("completed-event")){
		        eventData.setEventKeyUUID(eventKeyUUID.toString());
		        String duplicatekey = eventRowKey+"~"+eventRowKey;
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

    public void handleEventObjectMessage(EventObject eventObject) throws JSONException, ConnectionException, IOException, GeoIp2Exception{
	    try {
	    	
	    	Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObject);    	
	    	
	    	eventMap = this.formatEventMap(eventObject, eventMap);
	    	
	    	String apiKey = eventObject.getApiKey() != null ? eventObject.getApiKey() : DEFAULT_API_KEY;
	    	
	    	Map<String,Object> records = new HashMap<String, Object>();
	    	records.put("event_name", eventMap.get("eventName"));
	    	records.put("api_key",apiKey);
	    	Collection<String> eventId = baseDao.getKey(ColumnFamily.DIMEVENTS.getColumnFamily(),records);

	    	if(eventId == null || eventId.isEmpty()){
	    		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	    		records.put("event_id", uuid.toString());
	    		String key = apiKey +SEPERATOR+uuid.toString();
				 baseDao.saveMultipleStringValue(ColumnFamily.DIMEVENTS.getColumnFamily(),key,records);
			 }		
	    	
	    	updateEventObjectCompletion(eventObject);
	    	
			String eventKeyUUID = baseDao.saveEventObject(ColumnFamily.EVENTDETAIL.getColumnFamily(),null,eventObject);
			 
			if (eventKeyUUID == null) {
			    return;
			}
	      
			if (eventObject.getFields() != null) {
				logger.info("CORE: Writing to activity log - :"+ eventObject.getFields().toString());
				kafkaLogWriter.sendEventLog(eventObject.getFields());
				
			}
	
			Date eventDateTime = new Date(eventObject.getStartTime());
			String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
	
			if(eventObject.getEventType() == null || !eventObject.getEventType().equalsIgnoreCase("stop") || !eventObject.getEventType().equalsIgnoreCase("completed-event")){
			    baseDao.updateTimelineObject(ColumnFamily.EVENTTIMELINE.getColumnFamily(), eventRowKey,eventKeyUUID.toString(),eventObject);
			}
			
			String aggregatorJson = cache.get(eventMap.get("eventName"));
			
			logger.info("From cachee : {} ", cache.get(ATMOSPHERENDPOINT));
			
			if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){		 	
	
				liveAggregator.realTimeMetrics(eventMap, aggregatorJson);
	
				microAggregator.sendEventForAggregation(eventObject.getFields());
			
			}
		  
			if(aggregatorJson != null && !aggregatorJson.isEmpty() && aggregatorJson.equalsIgnoreCase(RAWUPDATE)){
				liveAggregator.updateRawData(eventMap);
			}
			liveDashBoardDAOImpl.callCountersV2(eventMap);
	
			if(cache.get(VIEWEVENTS).contains(eventMap.get("eventName"))){
				liveDashBoardDAOImpl.addContentForPostViews(eventMap);
			}
			
			liveDashBoardDAOImpl.findDifferenceInCount(eventMap);

			liveDashBoardDAOImpl.addApplicationSession(eventMap);

			liveDashBoardDAOImpl.saveGeoLocations(eventMap);		
			long start = System.currentTimeMillis();
			liveDashBoardDAOImpl.saveInESIndex(eventMap);
			long stop = System.currentTimeMillis();
			logger.info("Time Taken : {} ",(stop - start));
			if(pushingEvents.contains(eventMap.get("eventName"))){
				liveDashBoardDAOImpl.pushEventForAtmosphere(cache.get(ATMOSPHERENDPOINT),eventMap);
			}
			
			/*
			 * To be Re-enable 
			 * 
	
			  if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
				liveDashBoardDAOImpl.pushEventForAtmosphereProgress(atmosphereEndPoint, eventMap);
			}*/
	
			
    	}catch(Exception e){
    		logger.info("Exception in handleEventObjectHandler : {} ",e);
    	}
		
   }
    /**
     * 
     * @param eventData
     * 		Update the event is completion status 
     * @throws ConnectionException
     * 		If the host is unavailable
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

			ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventObject.getEventId());
			if (existingRecord != null && !existingRecord.isEmpty()) {
			    if ("stop".equalsIgnoreCase(eventObject.getEventType())) {
			        startTime = existingRecord.getLongValue("start_time", null);
			        //Update startTime with existingRecord, IF existingRecord.startTime < startTime
			    } else {
			        endTime = existingRecord.getLongValue("end_time", null);
			        // Update endTime with existing record IF existingRecord.endTime > endTime
			    }
			    eventComplete = true;
			}
			// Time taken for the event in milliseconds derived from the start / stop events.
			if (endTime != null && startTime != null) {
				timeInMillisecs = endTime - startTime;
			}
			if (timeInMillisecs > 1147483647) {
			    // When time in Milliseconds is very very huge, set to min time to serve the call.
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

        if(!StringUtils.isEmpty(eventObject.getParentEventId())){
        	ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventObject.getParentEventId());
        	if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
        		Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
        		baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventObject.getParentEventId(), "end_time", endTime);
        		baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventObject.getParentEventId(), "time_spent_in_millis", (endTime-parentStartTime));
        	}
        }

    }
    
    /**
     * 
     * @param startTime
     * @param endTime
     * @param customEventName
     * @throws ParseException
     */
    public void updateStaging(String startTime , String endTime,String customEventName,String apiKey) throws ParseException {
    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    	SimpleDateFormat dateIdFormatter = new SimpleDateFormat("yyyy-MM-dd 00:00:00+0000");
    	Calendar cal = Calendar.getInstance();
    	/*	MutationBatch m = null;
    	try {
			 m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	
    	String dateId = null;
    	String minuteId = null;
    	String hourId = null;
    	String eventId = null;
    	String userUid = null;
    	String processingDate = null;
    	long jobsCount = 0L;

    	jobsCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs_count").getLongValue();
    	if(jobsCount == 0 ){
    		jobsCount ++;
    		baseDao.generateNonCounter(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs-count", jobsCount, m);
    		baseDao.deleteAll(ColumnFamily.STAGING.getColumnFamily());
    		logger.info("Staging table truncated");
    	}else{
    		jobsCount ++;
    		baseDao.generateNonCounter(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs-count", jobsCount, m);
    		logger.info("Job is already running! so Staging table will not truncate");
    	}
    	
    	//Get all the event name and store for Caching
    	Map<String, String> events = new HashMap<String, String>();
    	Rows<String, String> eventRows = 	baseDao.readIndexedColumn(ColumnFamily.DIMEVENTS.getColumnFamily(),"api_key",apiKey != null ? apiKey : DEFAULT_API_KEY);
    	for (Row<String, String> eventRow : eventRows) {
			events.put(eventRow.getColumns().getStringValue("event_name", null),eventRow.getColumns().getStringValue("event_id", null));
		}
*/    	//Process records for every minute
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		int currentHour = dateFormatter.parse(startDate.toString()).getHours();
    		int currentMinute = dateFormatter.parse(startDate.toString()).getMinutes();
    		
    		logger.info("Porcessing Date : {}" , startDate.toString());
    		
    		//Get Time ID and Hour ID
/*    		Map<String,String> timeDetailMap = new LinkedHashMap<String, String>();
    		timeDetailMap.put("hour", String.valueOf(currentHour));
    		timeDetailMap.put("minute", String.valueOf(currentMinute));
    		Rows<String, String> timeDetails =  baseDao.readIndexedColumnList(ColumnFamily.DIMTIME.getColumnFamily(),timeDetailMap);

    		for(Row<String, String> timeIds : timeDetails){
	   			 minuteId = timeIds.getColumns().getStringValue("time_hm_id", null);
				 hourId = timeIds.getColumns().getLongValue("time_h_id", null).toString();
			 }
	   		 
   		 	if(!currentDate.equalsIgnoreCase(processingDate)){
   		 			processingDate = currentDate;
   		 		Rows<String, String> dateDetail = 	baseDao.readIndexedColumn(ColumnFamily.DIMDATE.getColumnFamily(),"date", currentDate);
   		 		for(Row<String, String> dateIds : dateDetail){
   		 			 dateId = dateIds.getKey().toString();
   		 		 }
   		 	}
   		 	
   		 	//Retry 100 times to get Date ID if Cassandra failed to respond
   		 	int dateTrySeq = 1;
   		 	while((dateId == null || dateId.equalsIgnoreCase("0")) && dateTrySeq < 100){
   		 	Rows<String, String> dateDetail = 	baseDao.readIndexedColumn(ColumnFamily.DIMDATE.getColumnFamily(),"date", currentDate);
		 		for(Row<String, String> dateIds : dateDetail){
		 			 dateId = dateIds.getKey().toString();
		 		 }
   		 		dateTrySeq++;
   		 	}
   		 	
   		 		Generate Key if loads custom Event Name
   		 	 */
   		 	String timeLineKey = null;   		 	
   		 	if(customEventName == null || customEventName  == "") {
   		 		timeLineKey = startDate.toString();
   		 	} else {
   		 		timeLineKey = startDate.toString()+"~"+customEventName;
   		 	}
   		 	
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey);
	    	if(eventUUID == null && eventUUID.isEmpty() ) {
	    		logger.info("No events in given timeline :  {}",startDate);
	    		return;
	    	}
	 
	    	Collection<String> eventDetailkeys = new ArrayList<String>();
	    	for(int i = 0 ; i < eventUUID.size() ; i++) {
	    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
	    		eventDetailkeys.add(eventDetailUUID);
	    	}
	    	
	    	//Read all records from Event Detail
	    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailkeys);
	    	for (Row<String, String> row : eventDetailsNew) {

	    		String fields = row.getColumns().getStringValue("fields", null);
	    		EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
	    		try {
					this.handleEventObjectMessage(eventObjects);
				} catch (Exception e) {
				logger.info("Error while Migration : {} ",e);
				}
	    		/*//Skip Invalid Events
	    		if(searchType == null ) {
	    			continue;
	    		}
	    		
	    		if(searchType.equalsIgnoreCase("session-expired")) {
	    			continue;
	    		}
	    		//Skip Duplicate events
	    		if(searchType.equalsIgnoreCase(LoaderConstants.CP.getName()) 
	    				|| searchType.equalsIgnoreCase(LoaderConstants.CPD.getName()) 
	    				|| searchType.equalsIgnoreCase(LoaderConstants.CRPD.getName()) 
	    				|| searchType.equalsIgnoreCase(LoaderConstants.CRP.getName())
	    				|| searchType.equalsIgnoreCase(LoaderConstants.CRPV1.getName())
	    				|| searchType.equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
	    			String eventType = row.getColumns().getStringValue("event_type", null);
	    			if(eventType != null) {
		    			if(eventType.equalsIgnoreCase("stop")){
		    				continue;
		    			}
	    			}
	    		}
	    		
	    		//Get Event ID for corresponding Event Name
	    		 eventId = events.get(searchType);
	    		 
	    		
	    		if(eventId == null) {
	    			continue;
	    		}
		    	//Get User ID	
	    		if(row.getColumns().getStringValue("gooru_uid", null) != null) {
					 userUid = row.getColumns().getStringValue("gooru_uid", null);
				 }else if (row.getColumns().getStringValue("userid", null) != null) {
					 try {
						if(row.getColumns().getStringValue("user_id", null) != null){
							ColumnList<String> userUidList = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), row.getColumns().getStringValue("user_id", null));
							userUid = userUidList.getStringValue("gooru_uid", null);
						}
					} catch (Exception e) {
						logger.info("Error while fetching User uid ");
					}
				 }
	    		
	    		//Save Staging records
    			HashMap<String, String> stagingEvents  = this.createStageEvents(minuteId,hourId,dateId, eventId, userUid, row.getColumns() , row.getKey());
    			baseDao.saveBulkStringList(ColumnFamily.STAGING.getColumnFamily(), stagingEvents.get("keys").toString(), stagingEvents);
    			
	    			String newEventName = DataUtils.makeCombinedEventName(searchType);
	    			if(!newEventName.equalsIgnoreCase(searchType)) {
	    				String newEventId = events.get(newEventName);
	    				HashMap<String, String> customStagingEvents  = this.createStageEvents(minuteId,hourId, dateId, newEventId, userUid, row.getColumns(),TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString());
	    				baseDao.saveBulkStringList(ColumnFamily.STAGING.getColumnFamily(), customStagingEvents.get("keys").toString(), customStagingEvents);
	    			}
	    		}
*/	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
	    	     	}
	    }
    	/*
   	jobsCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs_count").getLongValue();
    	jobsCount--;
    	baseDao.generateNonCounter(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs-count", jobsCount, m);
    	try {
			m.execute();
			logger.info("Process Ends  : Inserted successfully");
		} catch (ConnectionException e) {
			e.printStackTrace();
		}*/
    }

    public void postMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings");
    	ColumnList<String> jobIds = baseDao.readWithKey(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), "job_ids");
    	
    	long jobCount = Long.valueOf(settings.getColumnByName("running_job_count").getStringValue());
    	long totalJobCount = Long.valueOf(settings.getColumnByName("total_job_count").getStringValue());
    	long maxJobCount = Long.valueOf(settings.getColumnByName("max_job_count").getStringValue());
    	long allowedCount = Long.valueOf(settings.getColumnByName("allowed_count").getStringValue());
    	long indexedCount = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    	long totalTime = Long.valueOf(settings.getColumnByName("total_time").getStringValue());
    	String runningJobs = jobIds.getColumnByName("job_names").getStringValue();
    		
    	if((jobCount < maxJobCount) && (indexedCount < allowedCount) ){
    		long start = System.currentTimeMillis();
    		long endIndex = Long.valueOf(settings.getColumnByName("max_count").getStringValue());
    		long startVal = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    		long endVal = (endIndex + startVal);
    		jobCount = (jobCount + 1);
    		totalJobCount = (totalJobCount + 1);
    		String jobId = "job-"+UUID.randomUUID();
    		
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "start_count", ""+startVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "end_count", ""+endVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Inprogress");
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings", "total_job_count", ""+totalJobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings", "running_job_count", ""+jobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings", "indexed_count", ""+endVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), "job_ids", "job_names", runningJobs+","+jobId);
    		
    		Rows<String, String> resource = null;
    		MutationBatch m = null;
    		try {
    		m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

    		for(long i = startVal ; i < endVal ; i++){
    			logger.info("contentId : "+ i);
    				resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i);
    				if(resource != null && resource.size() > 0){
    					ColumnList<String> columns = resource.getRowByIndex(0).getColumns();
    					logger.info("Gooru Id: {} = Views : {} ",columns.getColumnByName("gooru_oid").getStringValue(),columns.getColumnByName("views_count").getLongValue());
    					baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(), "count~views", columns.getColumnByName("views_count").getLongValue(), m);
    					baseDao.generateNonCounter(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(), "status", "migrated", m);
    					baseDao.generateNonCounter(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(), "last_migrated", dateFormatter.format((new Date())).toString(), m);
    					baseDao.generateNonCounter(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(), "last_updated", columns.getColumnByName("last_modified").getStringValue(), m);
    					baseDao.generateNonCounter(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"views~"+i, "gooruOid", columns.getColumnByName("gooru_oid").getStringValue(), m);
    					
    				}
    			
    		}
    			m.execute();
    			long stop = System.currentTimeMillis();
    			
    			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Completed");
    			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "run_time", (stop-start)+" ms");
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings", "total_time", ""+(totalTime + (stop-start)));
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings", "running_job_count", ""+(jobCount - 1));
    			
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}else{    		
    		logger.info("Job queue is full! Or Job Reached its allowed end");
    	}
		
    }
    
public void postStatMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat_job_settings");
    	ColumnList<String> jobIds = baseDao.readWithKey(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"stat_job_ids");
    	
    	Collection<String> columnList = new ArrayList<String>();
    	columnList.add("count~views");
    	columnList.add("count~ratings");
    	
    	long jobCount = Long.valueOf(settings.getColumnByName("running_job_count").getStringValue());
    	long totalJobCount = Long.valueOf(settings.getColumnByName("total_job_count").getStringValue());
    	long maxJobCount = Long.valueOf(settings.getColumnByName("max_job_count").getStringValue());
    	long allowedCount = Long.valueOf(settings.getColumnByName("allowed_count").getStringValue());
    	long indexedCount = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    	long totalTime = Long.valueOf(settings.getColumnByName("total_time").getStringValue());
    	
    	String runningJobs = jobIds.getColumnByName("job_names").getStringValue();
    		
    	if((jobCount < maxJobCount) && (indexedCount < allowedCount) ){
    		long start = System.currentTimeMillis();
    		long endIndex = Long.valueOf(settings.getColumnByName("max_count").getStringValue());
    		long startVal = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    		long endVal = (endIndex + startVal);
    		jobCount = (jobCount + 1);
    		totalJobCount = (totalJobCount + 1);
    		String jobId = "job-"+UUID.randomUUID();
    		
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "start_count", ""+startVal);
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "end_count",  ""+endVal);
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Inprogress");
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"stat_job_ids", "job_names", runningJobs+","+jobId);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "total_job_count", ""+totalJobCount);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "running_job_count", ""+jobCount);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "indexed_count", ""+endVal);
    		
    		
    		JSONArray resourceList = new JSONArray();
    		try {
	    		for(long i = startVal ; i <= endVal ; i++){
	    			logger.info("contentId : "+ i);
	    			String gooruOid = null;
	    			Column<String> gooruOidColumnString = baseDao.readWithKeyColumn(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"views~"+i, "gooruOid");
	    			if(gooruOidColumnString != null){
	    				gooruOid = gooruOidColumnString.getStringValue();
	    			}
	    				logger.info("gooruOid : {}",gooruOid);
	    				if(gooruOid != null){
	    					ColumnList<String> vluesList = baseDao.readWithKeyColumnList(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+gooruOid, columnList);
	    					JSONObject resourceObj = new JSONObject();
	    					for(Column<String> detail : vluesList) {
	    						resourceObj.put("gooruOid", gooruOid);
	    						if(detail.getName().contains("views")){
	    							resourceObj.put("views", detail.getLongValue());
	    						}
	    						if(detail.getName().contains("ratings")){
	    							resourceObj.put("ratings", detail.getLongValue());
	    						}
	    						resourceObj.put("resourceType", "resource");
	    						logger.info("gooruOid : {}" , gooruOid);
	    					}
	    					resourceList.put(resourceObj);
	    				}
	    		}
	    		try{
	    			if((resourceList.length() != 0)){
	    				this.callStatAPI(resourceList, null);
	    			}
	    			long stop = System.currentTimeMillis();
	    			
	    			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Completed");
	    			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "run_time", (stop-start)+" ms");
	    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat_job_settings", "total_time", ""+(totalTime + (stop-start)));
	    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat_job_settings", "running_job_count", ""+(jobCount - 1));
	    			
	    		}catch(Exception e){
	    			logger.info("Error in search API : {}",e);
	    		}
	    		
    		} catch (Exception e) {
    			logger.info("Something went wrong : {}",e);
    		}
    	}else{    		
    		logger.info("Job queue is full!Or Reached maximum count!!");
    	}
		
    }
    
	public void balanceStatDataUpdate(){
		if(cache.get("balance_view_job").equalsIgnoreCase("stop")){
    		return;
    	}
		Calendar cal = Calendar.getInstance();
		try{
		MutationBatch m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "bal_stat_job_settings");
		for (Long startDate = Long.parseLong(settings.getStringValue("last_updated_time", null)) ; startDate <= Long.parseLong(minuteDateFormatter.format(new Date()));) {
			JSONArray resourceList = new JSONArray();
			logger.info("Start Date : {} ",String.valueOf(startDate));
			ColumnList<String> recentReources =  baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+String.valueOf(startDate));
			Collection<String> gooruOids =  recentReources.getColumnNames();
			
			for(String id : gooruOids){
				ColumnList<String> insightsData = baseDao.readWithKey(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+id);
				ColumnList<String> gooruData = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+id);
				long insightsView = 0L;
				long gooruView = 0L;
				if(insightsData != null){
					insightsView =   insightsData.getLongValue("count~views", 0L);
				}
				logger.info("insightsView : {} ",insightsView);
				if(gooruData != null){
					gooruView =  gooruData.getLongValue("views_count", 0L);
				}
				logger.info("gooruView : {} ",gooruView);
				long balancedView = (gooruView - insightsView);
				logger.info("Insights update views : {} ", (insightsView + balancedView) );
				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+id, "count~views", balancedView, m);
				if(balancedView != 0){
					logger.info("Generating resource Object : {}",balancedView);
					JSONObject resourceObj = new JSONObject();
					resourceObj.put("gooruOid", id);
					resourceObj.put("views", (insightsView + balancedView));
					resourceObj.put("resourceType", "resource");
					resourceList.put(resourceObj);
				}
			}
				m.execute();
				if(resourceList.length() != 0){
					this.callStatAPI(resourceList, null);
				}
				
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "bal_stat_job_settings", "last_updated_time", String.valueOf(startDate));
				
				cal.setTime(minuteDateFormatter.parse(String.valueOf(startDate)));
				cal.add(Calendar.MINUTE, 1);
				Date incrementedTime =cal.getTime(); 
				startDate = Long.parseLong(minuteDateFormatter.format(incrementedTime));
			}
    	}catch(Exception e){
			logger.info("Error in balancing view counts : {}",e);
		}
	
	}
    //Creating staging Events
    public HashMap<String, String> createStageEvents(String minuteId,String hourId,String dateId,String eventId ,String userUid,ColumnList<String> eventDetails ,String eventDetailUUID) {
    	HashMap<String, String> stagingEvents = new HashMap<String, String>();
    	stagingEvents.put("minuteId", minuteId);
    	stagingEvents.put("hourId", hourId);
    	stagingEvents.put("dateId", dateId);
    	stagingEvents.put("eventId", eventId);
    	stagingEvents.put("userUid", userUid);
    	stagingEvents.put("contentGooruOid",eventDetails.getStringValue("content_gooru_oid", null));
    	stagingEvents.put("parentGooruOid",eventDetails.getStringValue("parent_gooru_oid", null));
    	stagingEvents.put("timeSpentInMillis",eventDetails.getLongValue("time_spent_in_millis", 0L).toString());
    	stagingEvents.put("organizationUid",eventDetails.getStringValue("organization_uid", null));
    	stagingEvents.put("eventValue",eventDetails.getStringValue("event_value", null));
    	stagingEvents.put("resourceType",eventDetails.getStringValue("resource_type", null));
    	stagingEvents.put("appOid",eventDetails.getStringValue("app_oid", null));
    	stagingEvents.put("appUid",eventDetails.getStringValue("app_uid", null));
    	stagingEvents.put("city",eventDetails.getStringValue("city", null));
    	stagingEvents.put("state",eventDetails.getStringValue("state", null));
    	stagingEvents.put("country",eventDetails.getStringValue("country", null));
    	stagingEvents.put("attempt_number_of_try_sequence",eventDetails.getStringValue("attempt_number_of_try_sequence", null));
    	stagingEvents.put("attempt_first_status",eventDetails.getStringValue("attempt_first_status", null));
    	stagingEvents.put("answer_first_id",eventDetails.getStringValue("answer_first_id", null));
    	stagingEvents.put("attempt_status",eventDetails.getStringValue("attempt_status", null));
    	stagingEvents.put("attempt_try_sequence",eventDetails.getStringValue("attempt_try_sequence", null));
    	stagingEvents.put("answer_ids",eventDetails.getStringValue("answer_ids", null));
    	stagingEvents.put("open_ended_text",eventDetails.getStringValue("open_ended_text", null));
    	stagingEvents.put("keys",eventDetailUUID);
    	return stagingEvents; 
    }

    public void callAPIViewCount() throws JSONException {
    	if(cache.get("stat_job").equalsIgnoreCase("stop")){
    		logger.info("job stopped");
    		return;
    	}
    	JSONArray resourceList = new JSONArray();
    	String lastUpadatedTime = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~last~updated", DEFAULTCOLUMN).getStringValue();
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

		if((rowValues.getTime() <= currDate.getTime())){
			this.getRecordsToProcess(rowValues, resourceList);
			this.getRecordsToProcess(lastDate, resourceList);
			logger.info("processing mins : {} , {} ",minuteDateFormatter.format(rowValues),minuteDateFormatter.format(rowValues));
		}else{
			logger.info("processing min : {} ",currDate);
			this.getRecordsToProcess(currDate, resourceList);
		}
   }
  private void getRecordsToProcess(Date rowValues,JSONArray resourceList) throws JSONException{
	  

		ColumnList<String> contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+minuteDateFormatter.format(rowValues));		
		for(int i = 0 ; i < contents.size() ; i++) {
			ColumnList<String> vluesList = baseDao.readWithKeyColumnList(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+contents.getColumnByIndex(i).getName(), statKeys);
			for(Column<String> detail : vluesList) {
				JSONObject resourceObj = new JSONObject();
				resourceObj.put("gooruOid", contents.getColumnByIndex(i).getStringValue());
				for(String column : statKeys){
					if(detail.getName().equals(column)){
						logger.info("statValuess : {}",statMetrics.getStringValue(column, null));
						resourceObj.put(statMetrics.getStringValue(column, null), detail.getLongValue());
						resourceObj.put("resourceType", "resource");
					}
				}
				if(resourceObj.length() > 0 ){
					resourceList.put(resourceObj);
				}
			}
		}
		
		if((resourceList.length() != 0)){
			this.callStatAPI(resourceList, rowValues);
		}else{
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~last~updated", DEFAULTCOLUMN, minuteDateFormatter.format(rowValues));
	 		logger.info("No content viewed");
		}
	 
  }
    
    private void callStatAPI(JSONArray resourceList,Date rowValues){
    	JSONObject staticsObj = new JSONObject();
		String sessionToken = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN).getStringValue();
		try{
				String url = cache.get(VIEWUPDATEENDPOINT) + "?skipReindex=true&sessionToken=" + sessionToken;
				DefaultHttpClient httpClient = new DefaultHttpClient();   
				staticsObj.put("statisticsData", resourceList);
				logger.info("staticsObj : {}",staticsObj);
				StringEntity input = new StringEntity(staticsObj.toString());			        
		 		HttpPost  postRequest = new HttpPost(url);
		 		logger.info("staticsObj : {}",url);
		 		postRequest.addHeader("accept", "application/json");
		 		postRequest.setEntity(input);
		 		HttpResponse response = httpClient.execute(postRequest);
		 		logger.info("Status : {} ",response.getStatusLine().getStatusCode());
		 		logger.info("Reason : {} ",response.getStatusLine().getReasonPhrase());
		 		if (response.getStatusLine().getStatusCode() != 200) {
		 	 		logger.info("View count api call failed...");
		 	 		throw new AccessDeniedException("Something went wrong! Api fails");
		 		} else {
		 			if(rowValues != null){
		 				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~last~updated", DEFAULTCOLUMN, minuteDateFormatter.format(rowValues));
		 			}
		 	 		logger.info("View count api call Success...");
		 		}
		 			
		} catch(Exception e){
			e.printStackTrace();
		}		
	
    }
    
     public void updateActivityCompletion(String userUid, ColumnList<String> activityRow, String eventId, Map<String, Object> timeMap){
       	Long startTime = activityRow.getLongValue(START_TIME, 0L), endTime = activityRow.getLongValue(END_TIME, 0L);
       	String eventType = null;
       	JsonElement jsonElement = null;
       	JsonObject existingEventObj = null;
       	String existingColumnName = null;
       	
       	if (activityRow.getStringValue(EVENT_TYPE, null) != null){
       		eventType = activityRow.getStringValue(EVENT_TYPE, null);
       	}
       	
           long timeInMillisecs = 0L;
           if (endTime != null && startTime != null) {
               timeInMillisecs = endTime - startTime;
           }

           if (!StringUtils.isEmpty(eventType) && userUid != null) {
       		Map<String,Object> existingRecord = baseDao.isEventIdExists(ColumnFamily.ACITIVITYSTREAM.getColumnFamily(),userUid, eventId);
       		if(existingRecord.get("isExists").equals(true) && existingRecord.get("jsonString").toString() != null) {
   			    jsonElement = new JsonParser().parse(existingRecord.get("jsonString").toString());
   				existingEventObj = jsonElement.getAsJsonObject();
   			    if ("completed-event".equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType)) {
   					existingColumnName = existingRecord.get("existingColumnName").toString();
   				    startTime = existingEventObj.get(START_TIME).getAsLong();
   			    } else {
   				    endTime = existingEventObj.get(END_TIME).getAsLong();
   			    }
       		}
       		
   			// Time taken for the event in milliseconds derived from the start / stop events.
   			if (endTime != null && startTime != null) {
   				timeInMillisecs = endTime - startTime;
   			}
   			if (timeInMillisecs > 1147483647) {
   			    // When time in Milliseconds is very very huge, set to min time to serve the call.
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
    
    public void executeForEveryMinute(String startTime,String endTime){
    	logger.debug("start the static loader");
    	JSONObject jsonObject = new JSONObject();
    	try {
			jsonObject.put("startTime",startTime );
			jsonObject.put("endTime",endTime );
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	microAggregator.sendEventForStaticAggregation(jsonObject.toString());
    }
    
    public void watchSession(){
    	try {
			liveDashBoardDAOImpl.watchApplicationSession();
		} catch (ParseException e) {
			logger.info("Exception : {} ",e);
		}
    }
    
    public boolean createEvent(String eventName,String apiKey){

    	Map<String,Object> records = new HashMap<String, Object>();
    	apiKey = apiKey != null ? apiKey : DEFAULT_API_KEY;
		records.put("api_key", apiKey);
		records.put("event_name", eventName);
    	if(baseDao.isValueExists(ColumnFamily.DIMEVENTS.getColumnFamily(),records)){
				 
				UUID eventId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
				records.put("event_id", eventId.toString());
				String key = apiKey+SEPERATOR+eventId.toString();
				 baseDao.saveMultipleStringValue(ColumnFamily.DIMEVENTS.getColumnFamily(),key,records);
		return true;
    	}
    	return false;
    }
    
	public boolean validateSchedular(String ipAddress) {

		try {
			//ColumnList<String> columnList = configSettings.getColumnList("schedular~ip");
			ColumnList<String> columnList = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"schedular~ip");
			String configuredIp = columnList.getColumnByName("ip_address").getStringValue();
			if (configuredIp != null) {
				if (configuredIp.equalsIgnoreCase(ipAddress))
					return true;
			}
		} catch (Exception e) {
			logger.error(" unable to get the scedular IP " + e);
			return false;
		}
		return false;
	}
    
    private Map<String,String> formatEventMap(EventObject eventObject,Map<String,String> eventMap){
    	
    	String userUid = null;
    	String organizationUid = DEFAULT_ORGANIZATION_UID;
    	eventObject.setParentGooruId(eventMap.get("parentGooruId"));
    	eventObject.setContentGooruId(eventMap.get("contentGooruId"));
    	if(eventMap.containsKey("parentEventId") && eventMap.get("parentEventId") != null){
    		eventObject.setParentEventId(eventMap.get("parentEventId"));
    	}
    	eventObject.setTimeInMillSec(Long.parseLong(eventMap.get("totalTimeSpentInMs")));
    	eventObject.setEventType(eventMap.get("type"));
    	
    	if (eventMap != null && eventMap.get("gooruUId") != null && eventMap.containsKey("organizationUId") && (eventMap.get("organizationUId") == null ||  eventMap.get("organizationUId").isEmpty())) {
				 try {
					 userUid = eventMap.get("gooruUId");
					 Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid);
	   					for(Row<String, String> userDetail : userDetails){
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
    	eventMap.put("startTime",String.valueOf(eventObject.getStartTime()));
    	
    	return eventMap;
    }

    private void getAndSetAnswerStatus(EventData eventData){
    	if(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CQRPD.getName()) || eventData.getEventName().equalsIgnoreCase(LoaderConstants.QPD.getName())){
    		String answerStatus = null;
    			if(eventData.getAttemptStatus().length == 0){
    				answerStatus = LoaderConstants.SKIPPED.getName();
    				eventData.setAttemptFirstStatus(answerStatus);
    				eventData.setAttemptNumberOfTrySequence(eventData.getAttemptTrySequence().length);
    			}else {
	    			if(eventData.getAttemptStatus()[0] == 1){
	    				answerStatus = LoaderConstants.CORRECT.getName();
	    			}else if(eventData.getAttemptStatus()[0] == 0){
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
			ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getEventId());
			if (existingRecord != null && !existingRecord.isEmpty()) {
			    if ("stop".equalsIgnoreCase(eventData.getEventType())) {
			        startTime = existingRecord.getLongValue("start_time", null);
			        //Update startTime with existingRecord, IF existingRecord.startTime < startTime
			    } else {
			        endTime = existingRecord.getLongValue("end_time", null);
			        // Update endTime with existing record IF existingRecord.endTime > endTime
			    }
			    eventComplete = true;
			}
			// Time taken for the event in milliseconds derived from the start / stop events.
			if (endTime != null && startTime != null) {
				timeInMillisecs = endTime - startTime;
			}
			if (timeInMillisecs > 1147483647) {
			    // When time in Milliseconds is very very huge, set to min time to serve the call.
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

        if(!StringUtils.isEmpty(eventData.getParentEventId())){
        	ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getParentEventId());
        	if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
        		Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
        		baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), "end_time", endTime);
        		baseDao.saveLongValue(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventData.getParentEventId(), "time_spent_in_millis", (endTime-parentStartTime));
        		
        	}
        }

    }

 private void updateActivityStream(String eventId) throws JSONException {
       	
       	if (eventId != null){

   	    	Map<String,String> rawMap = new HashMap<String, String>();
   		    String apiKey = null;
   	    	String userName = null;
   	    	String dateId = null;
   	    	String userUid = null;
   	    	String contentGooruId = null;
   	    	String parentGooruId = null;
   	    	String organizationUid = null;
   	    	Date endDate = new Date();
   	    	
   	    	ColumnList<String> activityRow = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventId);	
   	    	if (activityRow != null){
   	    	String fields = activityRow.getStringValue(FIELDS, null);
  	         	
   	    	SimpleDateFormat minuteDateFormatter = new SimpleDateFormat(MINUTEDATEFORMATTER);
   	    	HashMap<String, Object> activityMap = new HashMap<String, Object>();
   	    	Map<String, Object> eventMap = new HashMap<String, Object>();       
   	    	if(activityRow.getLongValue(END_TIME, null) != null) {
   	    		endDate = new Date(activityRow.getLongValue(END_TIME, null));
   	    	} else {
   	    		endDate = new Date(activityRow.getLongValue(START_TIME, null));
   	    	}
       		dateId = minuteDateFormatter.format(endDate).toString();
   			Map<String , Object> timeMap = new HashMap<String, Object>();

   			//Get userUid
   			if (rawMap != null && rawMap.get("gooruUId") != null) {
   				 try {
   					 userUid = rawMap.get("gooruUId");
   					Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid);
   					for(Row<String, String> userDetail : userDetails){
   						userName = userDetail.getColumns().getStringValue("username", null);
   					}
   				 } catch (Exception e) {
   						logger.info("Error while fetching User uid ");
   				 }
   			 } else if (activityRow.getStringValue("gooru_uid", null) != null) {
   				try {
   					 userUid = activityRow.getStringValue("gooru_uid", null);
   					Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", activityRow.getStringValue("gooru_uid", null));
   					for(Row<String, String> userDetail : userDetails){
   						userName = userDetail.getColumns().getStringValue("username", null);
   					}
   				} catch (Exception e) {
   					logger.info("Error while fetching User uid ");
   				}			
   			 } else if (activityRow.getStringValue("user_id", null) != null) {
   				 try {
   					ColumnList<String> userUidList = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), activityRow.getStringValue("user_id", null));
					userUid = userUidList.getStringValue("gooru_uid", null);
					
   					Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", activityRow.getStringValue("gooru_uid", null));
   					for(Row<String, String> userDetail : userDetails){
   						userName = userDetail.getColumns().getStringValue("username", null);
   					}						
   				} catch (Exception e) {
   					logger.info("Error while fetching User uid ");
   				}
   			 } 	
   			if(userUid != null && eventId != null){
   				logger.info("userUid {} ",userUid);
   				this.updateActivityCompletion(userUid, activityRow, eventId, timeMap);
   			} else {
   				return;
   			}

   		    if(rawMap != null && rawMap.get(APIKEY) != null) {
   		    	apiKey = rawMap.get(APIKEY);
   		    } else if(activityRow.getStringValue(APIKEY, null) != null){
   		    	apiKey = activityRow.getStringValue(APIKEY, null);
   		    }
   		    if(rawMap != null && rawMap.get(CONTENTGOORUOID) != null){
   		    	contentGooruId = rawMap.get(CONTENTGOORUOID);
   		    } else if(activityRow.getStringValue(CONTENT_GOORU_OID, null) != null){
   		    	contentGooruId = activityRow.getStringValue(CONTENT_GOORU_OID, null);
   		    }
   		    if(rawMap != null && rawMap.get(PARENTGOORUOID) != null){
   		    	parentGooruId = rawMap.get(PARENTGOORUOID);
   		    } else if(activityRow.getStringValue(PARENT_GOORU_OID, null) != null){
   		    	parentGooruId = activityRow.getStringValue(PARENT_GOORU_OID, null);
   		    }
   		    if(rawMap != null && rawMap.get(ORGANIZATIONUID) != null){
   		    	organizationUid = rawMap.get(ORGANIZATIONUID);
   		    } else if (activityRow.getStringValue("organization_uid", null) != null){
   		    	organizationUid = activityRow.getStringValue("organization_uid", null);
   		    }
   	    	activityMap.put("eventId", eventId);
   	    	activityMap.put("eventName", activityRow.getStringValue(EVENT_NAME, null));
   	    	activityMap.put("userUid",userUid);
   	    	activityMap.put("dateId", dateId);
   	    	activityMap.put("userName", userName);
   	    	activityMap.put("apiKey", apiKey);
   	    	activityMap.put("organizationUid", organizationUid);
   	        activityMap.put("existingColumnName", timeMap.get("existingColumnName"));
   	        
   	    	eventMap.put("start_time", timeMap.get("startTime"));
   	    	eventMap.put("end_time", timeMap.get("endTime"));
   	    	eventMap.put("event_type", timeMap.get("event_type"));
   	        eventMap.put("timeSpent", timeMap.get("timeSpent"));
   	
   	    	eventMap.put("user_uid",userUid);
   	    	eventMap.put("username",userName);
   	    	eventMap.put("raw_data",activityRow.getStringValue(FIELDS, null));
   	    	eventMap.put("content_gooru_oid", contentGooruId);
   	    	eventMap.put("parent_gooru_oid", parentGooruId);
   	    	eventMap.put("organization_uid", organizationUid);
   	    	eventMap.put("event_name", activityRow.getStringValue(EVENT_NAME, null));
   	    	eventMap.put("event_value", activityRow.getStringValue(EVENT_VALUE, null));
   	
	    	eventMap.put("event_id", eventId);
	    	eventMap.put("api_key",apiKey);
	    	eventMap.put("organization_uid",organizationUid);
	    	
   	    	activityMap.put("activity", new JSONSerializer().serialize(eventMap));
   	    	
   	    	if(userUid != null){
   	    		baseDao.saveActivity(ColumnFamily.ACITIVITYSTREAM.getColumnFamily(), activityMap);
   	    	}
   	    	}
       	}
   	}
 
    @Async
    private String updateEvent(EventData eventData) {
    	ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(),eventData.getApiKey());
        String appOid = apiKeyValues.getStringValue("app_oid", null);
        if(eventData.getTimeSpentInMs() != null){
	          eventData.setTimeInMillSec(eventData.getTimeSpentInMs());
	     }
        return baseDao.saveEvent(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData);
    }    
    /**
     * @param connectionProvider the connectionProvider to set
     */
    public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
    	this.connectionProvider = connectionProvider;
    }
    
}

  
