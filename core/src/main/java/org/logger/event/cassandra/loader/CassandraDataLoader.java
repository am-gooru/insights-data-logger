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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import org.ednovo.data.model.TypeConverter;
import org.json.JSONException;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.APIDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.ActivityStreamDaoCassandraImpl;
import org.logger.event.cassandra.loader.dao.AggregateDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.CounterDetailsDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimDateDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimEventsDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimTimeDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimUserDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.EventDetailDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.JobConfigSettingsDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.LiveDashBoardDAOImpl;
import org.logger.event.cassandra.loader.dao.RealTimeOperationConfigDAOImpl;
import org.logger.event.cassandra.loader.dao.RecentViewedResourcesDAOImpl;
import org.logger.event.cassandra.loader.dao.TimelineDAOCassandraImpl;
import org.restlet.data.Form;
import org.restlet.resource.ClientResource;
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
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.util.TimeUUIDUtils;

import flexjson.JSONSerializer;

public class CassandraDataLoader implements Constants {

    private static final Logger logger = LoggerFactory
            .getLogger(CassandraDataLoader.class);
    private Keyspace cassandraKeyspace;
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
    private SimpleDateFormat minuteDateFormatter;
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    private CassandraConnectionProvider connectionProvider;
    
    private EventDetailDAOCassandraImpl eventDetailDao;
    
    private TimelineDAOCassandraImpl timelineDao;
    
    private DimEventsDAOCassandraImpl eventNameDao;
    
    private DimDateDAOCassandraImpl dimDate;
    
    private DimTimeDAOCassandraImpl dimTime;
    
    private AggregateDAOCassandraImpl stagingAgg;
    
    private DimUserDAOCassandraImpl dimUser;
    
    private APIDAOCassandraImpl apiDao;
    
    private JobConfigSettingsDAOCassandraImpl configSettings;
    
    private RecentViewedResourcesDAOImpl recentViewedResources;
    
    private KafkaLogProducer kafkaLogWriter;
  
    private CounterDetailsDAOCassandraImpl counterDetailsDao;
        
    private LiveDashBoardDAOImpl liveDashBoardDAOImpl;

    private ActivityStreamDaoCassandraImpl activityStreamDao;

    private RealTimeOperationConfigDAOImpl realTimeOperation;
    
    public static  Map<String,String> realTimeOperators;
    
    private MicroAggregatorProducer microAggregator;
    
    private static GeoLocation geo;
    
    private Gson gson = new Gson();
    
    /**
     * Get Kafka properties from Environment
     */
    public CassandraDataLoader() {
        this(null);
        
        String KAFKA_IP = System.getenv("INSIGHTS_KAFKA_IP");
        String KAFKA_PORT = System.getenv("INSIGHTS_KAFKA_PORT");
        String KAFKA_ZK_PORT = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
        String KAFKA_TOPIC = System.getenv("INSIGHTS_KAFKA_TOPIC");
        String KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
        String KAFKA_AGGREGATOR_TOPIC = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_TOPIC");
        String KAFKA_PRODUCER_TYPE = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
        
        kafkaLogWriter = new KafkaLogProducer(KAFKA_IP, KAFKA_ZK_PORT,  KAFKA_FILE_TOPIC, KAFKA_PRODUCER_TYPE);
        microAggregator = new MicroAggregatorProducer(KAFKA_IP, KAFKA_ZK_PORT,  KAFKA_AGGREGATOR_TOPIC, KAFKA_PRODUCER_TYPE);
    }

    public CassandraDataLoader(Map<String, String> configOptionsMap) {
        init(configOptionsMap);
        this.gson = new Gson();

        String KAFKA_IP = System.getenv("INSIGHTS_KAFKA_IP");
        String KAFKA_PORT = System.getenv("INSIGHTS_KAFKA_PORT");
        String KAFKA_ZK_PORT = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
        String KAFKA_TOPIC = System.getenv("INSIGHTS_KAFKA_TOPIC");
        String KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
        String KAFKA_AGGREGATOR_TOPIC = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_TOPIC");
        String KAFKA_PRODUCER_TYPE = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
        
        kafkaLogWriter = new KafkaLogProducer(KAFKA_IP, KAFKA_ZK_PORT,  KAFKA_FILE_TOPIC, KAFKA_PRODUCER_TYPE);
    }

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    /**
     * *
     * @param configOptionsMap
     * Initialize Coulumn Family
     */
    
    private void init(Map<String, String> configOptionsMap) {
    	
        this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

        this.setConnectionProvider(new CassandraConnectionProvider());
        this.getConnectionProvider().init(configOptionsMap);

        this.eventDetailDao = new EventDetailDAOCassandraImpl(getConnectionProvider());
        this.timelineDao = new TimelineDAOCassandraImpl(getConnectionProvider());
        this.dimDate = new DimDateDAOCassandraImpl(getConnectionProvider());
 	    this.dimTime = new DimTimeDAOCassandraImpl(getConnectionProvider());
 	    this.eventNameDao = new DimEventsDAOCassandraImpl(getConnectionProvider());
 	    this.stagingAgg = new AggregateDAOCassandraImpl(getConnectionProvider());
        this.dimUser = new DimUserDAOCassandraImpl(getConnectionProvider());
        this.apiDao = new APIDAOCassandraImpl(getConnectionProvider());
        this.configSettings = new JobConfigSettingsDAOCassandraImpl(getConnectionProvider());    
        this.counterDetailsDao = new CounterDetailsDAOCassandraImpl(getConnectionProvider());
        this.liveDashBoardDAOImpl = new LiveDashBoardDAOImpl(getConnectionProvider());
        this.recentViewedResources = new RecentViewedResourcesDAOImpl(getConnectionProvider());
        this.activityStreamDao = new ActivityStreamDaoCassandraImpl(getConnectionProvider());
        this.realTimeOperation = new RealTimeOperationConfigDAOImpl(getConnectionProvider());
        realTimeOperators = realTimeOperation.getOperators();
        geo = new GeoLocation();
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
    	
    	this.findGeoLocation(eventData);
    	
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
	        	 existingRecord = eventDetailDao.readEventDetail(eventData.getEventId());
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
         
	         ColumnList<String> existingEventRecord = eventNameDao.readEventName(eventData);
	
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
				eventDetailDao.saveEvent(eventData, "GLP");
				kafkaLogWriter.sendEventLog(eventData.getFields());
				logger.info("CORE: Writing to activity log - :"+ eventData.getFields().toString());
			}
	    
	
	        // Insert into event_timeline column family
	        Date eventDateTime = new Date(eventData.getStartTime());
	        String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
	        if(eventData.getEventType() == null || !eventData.getEventType().equalsIgnoreCase("completed-event")){
		        eventData.setEventKeyUUID(eventKeyUUID.toString());
		        String duplicatekey = eventRowKey+"~"+eventRowKey;
		        timelineDao.updateTimeline(eventData, eventRowKey);
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
    
    	String userUid = null;
    	String organizationUid = null;
    	
    	Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObject);
    	
    	
    	eventObject.setParentGooruId(eventMap.get("parentGooruId"));
    	eventObject.setContentGooruId(eventMap.get("contentGooruId"));
    	eventObject.setTimeInMillSec(Long.parseLong(eventMap.get("totalTimeSpentInMs")));
    	eventObject.setEventType(eventMap.get("type"));
    	
    	if (eventMap != null && eventMap.get("gooruUId") != null && eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null &&  !eventMap.get("organizationUId").isEmpty()) {
				 try {
					 userUid = eventMap.get("gooruUId");
					 organizationUid = dimUser.getOrganizationUid(userUid);
					 eventObject.setOrganizationUid(organizationUid);
			    	 eventMap.put("organizationUId",eventObject.getOrganizationUid());
				 } catch (Exception e) {
						logger.info("Error while fetching User uid ");
				 }
			 }
    	if(eventObject.getUserIp() != null && !eventObject.getUserIp().isEmpty()){
    		eventMap.put("userIp", eventObject.getUserIp());
    	}
    	eventMap.put("eventName", eventObject.getEventName());
    	eventMap.put("eventId", eventObject.getEventId());
    	eventMap.put("startTime",String.valueOf(eventObject.getStartTime()));
    	String existingEventRecord = eventNameDao.getEventId(eventMap.get("eventName"));
		 if(existingEventRecord == null || existingEventRecord.isEmpty()){
			 eventNameDao.saveEventNameByName(eventObject.getEventName());
		 }
		
		 try {
			updateEventObjectCompletion(eventObject);
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		 
		 String eventKeyUUID = eventDetailDao.saveEventObject(eventObject);
		 
		if (eventKeyUUID == null) {
		    return;
		}
      
		if (eventObject.getFields() != null) {
			logger.info("Push events for atmosphere ");
			//this.pushMessage(eventObject.getFields().toString());
			logger.info("CORE: Writing to activity log - :"+ eventObject.getFields().toString());
			kafkaLogWriter.sendEventLog(eventObject.getFields());
		}
   

		// Insert into event_timeline column family
		Date eventDateTime = new Date(eventObject.getStartTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();

		if(eventObject.getEventType() == null || !eventObject.getEventType().equalsIgnoreCase("stop") || !eventObject.getEventType().equalsIgnoreCase("completed-event")){
		    timelineDao.updateTimelineObject(eventObject, eventRowKey,eventKeyUUID.toString());
		}
		
		//To be revoked
		long startEvent = System.currentTimeMillis();
		EventData eventData= getAndSetEventData(eventMap);
		this.updateEvent(eventData); 
		long stopEvent = System.currentTimeMillis();
		logger.info("Get and Set Event Data : {} ",(stopEvent - startEvent));
		
		String aggregatorJson = realTimeOperators.get(eventMap.get("eventName"));
		
		if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){		 	
			long startCounterDetail = System.currentTimeMillis();
			counterDetailsDao.realTimeMetrics(eventMap, aggregatorJson);
			long stopCounterDetail = System.currentTimeMillis();
			logger.info("counterDetail : {} ",(stopCounterDetail - startCounterDetail));
			microAggregator.sendEventForAggregation(eventObject.getFields());
		}
	  
		if(aggregatorJson != null && !aggregatorJson.isEmpty() && aggregatorJson.equalsIgnoreCase(RAWUPDATE)){
			counterDetailsDao.updateRawData(eventMap);
		}
		logger.info("userIp : {} ",eventMap.get("userIp"));
		//Track activities
		
		if(eventMap.containsKey("userIp") && eventMap.get("userIp") != null && !eventMap.get("userIp").isEmpty()){
			String city = geo.getGeoCityByIP(eventMap.get("userIp"));
        	String country = geo.getGeoCountryByIP(eventMap.get("userIp"));
			String state = geo.getGeoRegionByIP(eventMap.get("userIp"));
			logger.info("city : {} : country : {}",city,country);
			logger.info("state : {}",state);
		}
		long startActivity = System.currentTimeMillis();
	  	try {
    		liveDashBoardDAOImpl.callCounters(eventMap);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		long stopActivity = System.currentTimeMillis();
		logger.info("Activity : {} ",(stopActivity - startActivity));
    }
    /**
     * 
     * @param eventData
     * 		Update the event is completion status 
     * @throws ConnectionException
     * 		If the host is unavailable
     */
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
			ColumnList<String> existingRecord = eventDetailDao.readEventDetail(eventData.getEventId());
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
        	ColumnList<String> existingParentRecord = eventDetailDao.readEventDetail(eventData.getParentEventId());
        	if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
        		Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
        		eventDetailDao.updateParentId(eventData.getParentEventId(), endTime, (endTime-parentStartTime));
        	}
        }

    }
    
    @Async
    private void updateEventObjectCompletion(EventObject eventObejct) throws ConnectionException {

    	Long endTime = eventObejct.getEndTime(), startTime = eventObejct.getStartTime();
        long timeInMillisecs = 0L;
        if (endTime != null && startTime != null) {
            timeInMillisecs = endTime - startTime;
        }
        boolean eventComplete = false;

        eventObejct.setTimeInMillSec(timeInMillisecs);

        if (StringUtils.isEmpty(eventObejct.getEventId())) {
            return;
        }

			ColumnList<String> existingRecord = eventDetailDao.readEventDetail(eventObejct.getEventId());
			if (existingRecord != null && !existingRecord.isEmpty()) {
			    if ("stop".equalsIgnoreCase(eventObejct.getEventType())) {
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

			eventObejct.setStartTime(startTime);
			eventObejct.setEndTime(endTime);

        if (eventComplete) {
        	eventObejct.setTimeInMillSec(timeInMillisecs);
            eventObejct.setEventType("completed-event");
            eventObejct.setEndTime(endTime);
            eventObejct.setStartTime(startTime);
        }

        if(!StringUtils.isEmpty(eventObejct.getParentEventId())){
        	ColumnList<String> existingParentRecord = eventDetailDao.readEventDetail(eventObejct.getParentEventId());
        	if (existingParentRecord != null && !existingParentRecord.isEmpty()) {
        		Long parentStartTime = existingParentRecord.getLongValue("start_time", null);
        		eventDetailDao.updateParentId(eventObejct.getParentEventId(), endTime, (endTime-parentStartTime));
        	}
        }

    }
    
    /**
     * 
     * @param eventData
     * @return
     * Save Event Data
     */
    
    @Async
    private String updateEvent(EventData eventData) {
    	ColumnList<String> apiKeyValues = apiDao.readApiData(eventData.getApiKey());
        String appOid = apiKeyValues.getStringValue("app_oid", null);
        if(eventData.getTimeSpentInMs() != null){
	          eventData.setTimeInMillSec(eventData.getTimeSpentInMs());
	     }
        return eventDetailDao.saveEvent(eventData,appOid);
    }
   
    /**
     * 
     * @param startTime
     * @param endTime
     * @param customEventName
     * @throws ParseException
     */
    public void updateStaging(String startTime , String endTime,String customEventName) throws ParseException {
    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    	SimpleDateFormat dateIdFormatter = new SimpleDateFormat("yyyy-MM-dd 00:00:00+0000");
    	Calendar cal = Calendar.getInstance();
    	
    	String dateId = null;
    	String minuteId = null;
    	String hourId = null;
    	String eventId = null;
    	String userUid = null;
    	String processingDate = null;
    	long jobsCount = 0L;
    
    	//Check if already job is running
    	jobsCount = configSettings.getJobsCount();
    	
    	if(jobsCount == 0 ){
    		jobsCount ++;
    		configSettings.balancingJobsCount(jobsCount);
    		logger.info("Staging table truncated");
    		
    		stagingAgg.deleteAll();
    	}else{
    		jobsCount ++;
    		configSettings.balancingJobsCount(jobsCount);
    		logger.info("Job is already running! so Staging table will not truncate");
    	}
    	
    	//Get all the event name and store for Caching
    	HashMap<String, String> events = eventNameDao.readAllEventNames();
    	
    	//Process records for every minute
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		int currentHour = dateFormatter.parse(startDate.toString()).getHours();
    		int currentMinute = dateFormatter.parse(startDate.toString()).getMinutes();
    		
    		logger.info("Porcessing Date : {}" , startDate.toString());
    		
    		//Get Time ID and Hour ID
    		Rows<String, String> timeDetails = dimTime.getTimeId(""+currentHour,""+currentMinute);	
	   		 
    		 for(Row<String, String> timeIds : timeDetails){
	   			 minuteId = timeIds.getColumns().getStringValue("time_hm_id", null);
				 hourId = timeIds.getColumns().getLongValue("time_h_id", null).toString();
			 }
	   		 
   		 	if(!currentDate.equalsIgnoreCase(processingDate)){
   		 			processingDate = currentDate;
   		 			dateId = dimDate.getDateId(currentDate);
   		 	}
   		 	
   		 	//Retry 100 times to get Date ID if Cassandra failed to respond
   		 	int dateTrySeq = 1;
   		 	while((dateId == null || dateId.equalsIgnoreCase("0")) && dateTrySeq < 100){
   		 		dateId = dimDate.getDateId(currentDate);
   		 		dateTrySeq++;
   		 	}
   		 	
   		 	//Generate Key if loads custom Event Name
   		 	String timeLineKey = null;   		 	
   		 	if(customEventName == null || customEventName  == "") {
   		 		timeLineKey = startDate.toString();
   		 	} else {
   		 		timeLineKey = startDate.toString()+"~"+customEventName;
   		 	}
   		 	
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = timelineDao.readTimeLine(timeLineKey);
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
	    	Rows<String, String> eventDetailsNew = eventDetailDao.readEventDetailList(eventDetailkeys);
	    	for (Row<String, String> row : eventDetailsNew) {
	    		row.getColumns().getStringValue("event_name", null);
	    		String searchType = row.getColumns().getStringValue("event_name", null);
	    		
	    		//Skip Invalid Events
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
						userUid = dimUser.getUserUid(row.getColumns().getStringValue("user_id", null));
					} catch (ConnectionException e) {
						logger.info("Error while fetching User uid ");
					}
				 }
	    		
	    		//Save Staging records
    			HashMap<String, String> stagingEvents  = this.createStageEvents(minuteId,hourId,dateId, eventId, userUid, row.getColumns() , row.getKey());
    			stagingAgg.saveAggregation(stagingEvents);
	    			String newEventName = DataUtils.makeCombinedEventName(searchType);
	    			if(!newEventName.equalsIgnoreCase(searchType)) {
	    				String newEventId = events.get(newEventName);
	    				HashMap<String, String> customStagingEvents  = this.createStageEvents(minuteId,hourId, dateId, newEventId, userUid, row.getColumns(),TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString());
	    				stagingAgg.saveAggregation(customStagingEvents);
	    			}
	    		}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
    	}
    	
    	jobsCount = configSettings.getJobsCount();jobsCount--;
    	configSettings.balancingJobsCount(jobsCount);
    	logger.info("Process Ends  : Inserted successfully");
    }

    public void postMigration(String startTime , String endTime,String customEventName) throws ParseException{

    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    	SimpleDateFormat dateIdFormatter = new SimpleDateFormat("yyyy-MM-dd 00:00:00+0000");
    	Calendar cal = Calendar.getInstance();
    	
    	String dateId = null;
    	String minuteId = null;
    	String hourId = null;
    	String eventId = null;
    	String userUid = null;
    	String processingDate = null;
    	
    	//Get all the event name and store for Caching
    	HashMap<String, String> events = eventNameDao.readAllEventNames();
    	
    	//Process records for every minute
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		int currentHour = dateFormatter.parse(startDate.toString()).getHours();
    		int currentMinute = dateFormatter.parse(startDate.toString()).getMinutes();
    		
    		logger.info("Porcessing Date : {}" , startDate.toString());
    		
    		//Get Time ID and Hour ID
    		Rows<String, String> timeDetails = dimTime.getTimeId(""+currentHour,""+currentMinute);	
	   		 
    		 for(Row<String, String> timeIds : timeDetails){
	   			 minuteId = timeIds.getColumns().getStringValue("time_hm_id", null);
				 hourId = timeIds.getColumns().getLongValue("time_h_id", null).toString();
			 }
	   		 
   		 	if(!currentDate.equalsIgnoreCase(processingDate)){
   		 			processingDate = currentDate;
   		 			dateId = dimDate.getDateId(currentDate);
   		 	}
   		 	
   		 	//Retry 100 times to get Date ID if Cassandra failed to respond
   		 	int dateTrySeq = 1;
   		 	while((dateId == null || dateId.equalsIgnoreCase("0")) && dateTrySeq < 100){
   		 		dateId = dimDate.getDateId(currentDate);
   		 		dateTrySeq++;
   		 	}
   		 	
   		 	//Generate Key if loads custom Event Name
   		 	String timeLineKey = null;   		 	
   		 	if(customEventName == null || customEventName  == "") {
   		 		timeLineKey = startDate.toString();
   		 	} else {
   		 		timeLineKey = startDate.toString()+"~"+customEventName;
   		 	}
   		 	
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = timelineDao.readTimeLine(timeLineKey);
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
	    	Rows<String, String> eventDetailsNew = eventDetailDao.readEventDetailList(eventDetailkeys);
	    	for (Row<String, String> row : eventDetailsNew) {
	    		row.getColumns().getStringValue("event_name", null);
	    		String eventJSON = row.getColumns().getStringValue("fields", null);
	    		logger.info("eventJSON : {} ",eventJSON);
	        	String organizationUid = null;
	        	JsonObject eventObj = new JsonParser().parse(eventJSON).getAsJsonObject();
	        	EventObject eventObject = gson.fromJson(eventObj, EventObject.class);
	        	Map<String, String> eventMap = new HashMap<String, String>();
				try {
					eventMap = JSONDeserializer.deserializeEventObject(eventObject);
				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	        	
	        	eventObject.setParentGooruId(eventMap.get("parentGooruId"));
	        	eventObject.setContentGooruId(eventMap.get("contentGooruId"));
	        	eventObject.setTimeInMillSec(Long.parseLong(eventMap.get("totalTimeSpentInMs")));
	        	eventObject.setEventType(eventMap.get("type"));
	        	if (eventMap != null && eventMap.get("gooruUId") != null) {
	    				 try {
	    					 userUid = eventMap.get("gooruUId");
	    					 organizationUid = dimUser.getOrganizationUid(userUid);
	    					 eventObject.setOrganizationUid(organizationUid);
	    				 } catch (Exception e) {
	    						logger.info("Error while fetching User uid ");
	    				 }
	    			 }
	        	eventMap.put("organizationUId",eventObject.getOrganizationUid());
	        	eventMap.put("eventName", eventObject.getEventName());
	        	eventMap.put("eventId", eventObject.getEventId());
	        	eventMap.put("startTime",String.valueOf(eventObject.getStartTime()));
	        	counterDetailsDao.migrationAndUpdate(eventMap);
	    		}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
    	}
    	logger.info("Process Ends  : Inserted successfully");
    
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
    
    /**
     * 
     * @param startTime
     * @param endTime
     * @throws ParseException
     */
    public void updateGeoLocation (String startTime , String endTime) throws ParseException {
    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    	SimpleDateFormat dateIdFormatter = new SimpleDateFormat("yyyy-MM-dd 00:00:00+0000");
    	Calendar cal = Calendar.getInstance();
    	
    	String dateId = null;
    	String minuteId = null;
    	String hourId = null;
    	String eventId = null;
    	String userUid = null;

		logger.info("Geo-location updation started...");
    	
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		int currentHour = dateFormatter.parse(startDate.toString()).getHours();
    		int currentMinute = dateFormatter.parse(startDate.toString()).getMinutes();
    		
    		logger.info("Geo-location Porcessing Date : {}" , startDate.toString());
    		
    		Rows<String, String> timeDetails = dimTime.getTimeId(""+currentHour,""+currentMinute);
    		
	   		 for(Row<String, String> timeIds : timeDetails){
	   			 minuteId = timeIds.getColumns().getStringValue("time_hm_id", null);
				 hourId = timeIds.getColumns().getLongValue("time_h_id", null).toString();
			 }

	   		 ColumnList<String> eventUUID = timelineDao.readTimeLine(startDate.toString());
	    	
	    	if(eventUUID == null && eventUUID.isEmpty() ) {
	    		logger.info("No events in given timeline :  {}",startDate);
	    		return;
	    	}

	    	for(int i = 0 ; i < eventUUID.size() ; i++) {
	    		
	    		UUID stagingKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
	    		ColumnList<String> eventDetails = eventDetailDao.readEventDetail(eventDetailUUID);
	    		String user_ip = eventDetails.getStringValue("user_ip", null);
	    		if(user_ip == null ){
	    			continue;
	    		}
	    		user_ip = user_ip.trim();
	    		String ip = user_ip;
	    		String city = "";
	    		String state = "";
	    		String country = "";
	        	GeoLocation geo = new GeoLocation();
    	    	try {
    				city = geo.getGeoCityByIP(ip);
    	        	country = geo.getGeoCountryByIP(ip);
    				state = geo.getGeoRegionByIP(ip);
    			} catch (IOException e) {
    		        logger.info("Geo-location : Exception fetching geo location {} ", e);
    			} catch (GeoIp2Exception e) {
    		        logger.info("Geo-location : Exception fetching geo location {} ", e);
    			}
	    		
	    		eventDetailDao.saveGeoLocation(eventDetailUUID, city, state, country); 
	    	}
	    	
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
	    	
    	}
    	logger.info("Geo-location : Process Ends - Updated  successfully");
    }

    /**
     * 
     * @param gooruoid
     * @param viewcount
     * 		To update real time view count 
     */
    public void updateViewCount(String gooruoid, long viewcount ) {
    	counterDetailsDao.updateCounter(gooruoid,LoaderConstants.VIEWS.getName(), viewcount );
    }
    
    public void addAggregators(String eventName, String json ,String updateBy) {
    	realTimeOperation.addAggregators(eventName,json, updateBy);
    }
    /**
     *  Update bulk view count
     */
    public void callAPIViewCount() {
    	Rows<String, String> dataDetail = null;
		Long count = 0L;
		dataDetail = recentViewedResources.readAll();
				
		List<Map<String, Object>> dataJSONList = new ArrayList<Map<String, Object>>();
		for (Row<String, String> row : dataDetail) {
			Map<String, Object> map = new HashMap<String, Object>();
			String recentResources = row.getKey();
			count = counterDetailsDao.getCounterLongValue(recentResources, "resource-view");
			map.put("gooruOid", recentResources);
			map.put("views", count);
			dataJSONList.add(map);
		}
		logger.info("Serilizer : {}",new JSONSerializer().serialize(dataJSONList));
		boolean flagError = false;
		String sessionToken = configSettings.getConstants(LoaderConstants.SESSIONTOKEN.getName(),"constant_value");
		String VIEW_COUNT_REST_API_END_POINT = configSettings.getConstants(LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(),"constant_value");
		String result = null;
		
		try{
				String url = VIEW_COUNT_REST_API_END_POINT + "?sessionToken=" + sessionToken;
				
				DefaultHttpClient httpClient = new DefaultHttpClient();   
		        StringEntity input = new StringEntity(new JSONSerializer().serialize(dataJSONList).toString());
		 		HttpPost  postRequest = new HttpPost(url);
		 		postRequest.addHeader("accept", "application/json");
		 		postRequest.setEntity(input);
		 		HttpResponse response = httpClient.execute(postRequest);
		 		
		 		if (response.getStatusLine().getStatusCode() != 200) {
		 	 		logger.info("View count api call failed...");
		 			flagError = true;
		 	 		throw new AccessDeniedException("You can not delete resources! Api fails");
		 		} else {
		 	 		logger.info("View count api call Success...");
		 		}
		 			
		} catch(Exception e){
			e.printStackTrace();
		}
        
		
		if (!flagError) {
			for (Row<String, String> row : dataDetail) {
				String recentResources = row.getKey();
				recentViewedResources.deleteRow(recentResources);
			}
		}
		
    }
  
    /**
     * 
     * @param eventName
     * @throws Exception 
     */
    public void runPig(String eventName){
    	//will come in next release
    }
    
    protected static String convertStreamToString(InputStream is) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }
    
    private static void findGeoLocation(EventData eventData){
    	
    	String city = null;
       	String country = null ;
       	String state = null ;
       	String ip = "" ;

    	if(eventData.getUserIp() != null){
	    	try {
	    		ip = eventData.getUserIp().trim();
				city = geo.getGeoCityByIP(ip);
	        	country = geo.getGeoCountryByIP(ip);
				state = geo.getGeoRegionByIP(ip);
			} catch (IOException e) {
		        logger.info("Exception fetching geo location {} ", e);
		        return;
			} catch (GeoIp2Exception e) {
		        logger.info("Exception fetching geo location {} ", e);
		        return;
			}
    	}
    	
		if(city != null){
			eventData.setCity(city);
		}
		if(state !=null){
			eventData.setState(state);
		}
		if(country != null){
			eventData.setCountry(country);
		}
		
    	
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
    
    public EventData getAndSetEventData(Map<String,String> eventMap) throws JSONException{
    	EventData eventData = new EventData();
    	int[] attemptStatus = TypeConverter.stringToIntArray(eventMap.get("attemptStatus"));
    	int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get("attemptTrySequence"));
    	int[] answers = TypeConverter.stringToIntArray(eventMap.get("answers"));
    	String openEndedText = eventMap.get("text");
    	
    		String answerStatus = null;
    		if(attemptStatus != null && attemptTrySequence != null){
    			if(attemptStatus.length == 0){
    				answerStatus = LoaderConstants.SKIPPED.getName();
    				eventData.setAttemptFirstStatus(answerStatus);
    				eventData.setAttemptNumberOfTrySequence(attemptTrySequence.length);
    			}else {
	    			if(attemptStatus[0] == 1){
	    				answerStatus = LoaderConstants.CORRECT.getName();
	    			}else if(attemptStatus[0] == 0){
	    				answerStatus = LoaderConstants.INCORRECT.getName();
	    			}
	    			eventData.setAttemptFirstStatus(answerStatus);
    				eventData.setAttemptNumberOfTrySequence(attemptTrySequence.length);
    				eventData.setAnswerFirstId(answers[0]);
    				}
    			}
    			eventData.setEventName(eventMap.get("eventName"));
    			eventData.setEventId(eventMap.get("eventId"));
    			eventData.setContentGooruId(eventMap.get("contentGooruId"));
    			eventData.setParentGooruId(eventMap.get("parentGooruId"));
    			eventData.setParentEventId(eventMap.get("parentEventId"));
    			eventData.setFields(eventMap.get("fields"));
    			eventData.setReactionType(eventMap.get("reactionType"));
    			eventData.setQuery(eventMap.get("reactionType"));
    			eventData.setAttemptStatus(attemptStatus);
    			eventData.setAttemptTrySequence(attemptTrySequence);
    			eventData.setAnswerId(answers);
    			eventData.setOpenEndedText(openEndedText);
    			eventData.setOrganizationUid(eventMap.get("organizationUId"));
    			eventData.setType(eventMap.get("type"));
    			eventData.setGooruUId(eventMap.get("gooruUId"));
    			eventData.setApiKey(eventMap.get("apiKey"));
    			
		return eventData;
    }
    private static String loadStream(InputStream s) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(s));
        StringBuilder sb = new StringBuilder();
        String line;
        while((line=br.readLine()) != null)
            sb.append(line).append("\n");
        return sb.toString();
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
   	    	
   	    	ColumnList<String> activityRow = eventDetailDao.readEventDetail(eventId);	
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
   					 userName = dimUser.getUserName(userUid);
   				 } catch (ConnectionException e) {
   						logger.info("Error while fetching User uid ");
   				 }
   			 } else if (activityRow.getStringValue("gooru_uid", null) != null) {
   				try {
   					 userUid = activityRow.getStringValue("gooru_uid", null);
   					 userName = dimUser.getUserName(activityRow.getStringValue("gooru_uid", null)); 
   				} catch (ConnectionException e) {
   					logger.info("Error while fetching User uid ");
   				}			
   			 } else if (activityRow.getStringValue("user_id", null) != null) {
   				 try {
   					userUid = dimUser.getUserUid(activityRow.getStringValue("user_id", null));
   					userName = dimUser.getUserName(activityRow.getStringValue("user_id", null));						
   				} catch (ConnectionException e) {
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
   	    		activityStreamDao.saveActivity(activityMap);
   	    	}
   	    	}
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
       		Map<String,Object> existingRecord = activityStreamDao.isEventIdExists(userUid, eventId);
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

       @Async
       public void pushMessage(String fields){
    	ClientResource clientResource = null;
    	clientResource = new ClientResource("http://dev-insights.goorulearning.org/insights-api-dev/atmosphere/push/message");
    	Form forms = new Form();
		forms.add("data", fields);
		clientResource.post(forms.getWebRepresentation());
		
       }
    /**
     * @return the connectionProvider
     */
    public CassandraConnectionProvider getConnectionProvider() {
    	return connectionProvider;
    }
    
    /**
     * @param connectionProvider the connectionProvider to set
     */
    public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
    	this.connectionProvider = connectionProvider;
    }
    
    private ColumnFamilyQuery<String, String> prepareQuery(ColumnFamily<String, String> columnFamily) {
    	return cassandraKeyspace.prepareQuery(columnFamily).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    }
    
}

