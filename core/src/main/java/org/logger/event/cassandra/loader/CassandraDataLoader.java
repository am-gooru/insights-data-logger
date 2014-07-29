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
import org.ednovo.data.model.GeoData;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.TypeConverter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.dao.APIDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.ActivityStreamDaoCassandraImpl;
import org.logger.event.cassandra.loader.dao.AggregateDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.cassandra.loader.dao.DimDateDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimEventsDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimResourceDAOImpl;
import org.logger.event.cassandra.loader.dao.DimTimeDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.DimUserDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.EventDetailDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.JobConfigSettingsDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.LiveDashBoardDAOImpl;
import org.logger.event.cassandra.loader.dao.MicroAggregatorDAOmpl;
import org.logger.event.cassandra.loader.dao.RealTimeOperationConfigDAOImpl;
import org.logger.event.cassandra.loader.dao.RecentViewedResourcesDAOImpl;
import org.logger.event.cassandra.loader.dao.TimelineDAOCassandraImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.access.AccessDeniedException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.util.TimeUUIDUtils;

import flexjson.JSONSerializer;

public class CassandraDataLoader implements Constants {

    private static final Logger logger = LoggerFactory
            .getLogger(CassandraDataLoader.class);
    private Keyspace cassandraKeyspace;
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
    private SimpleDateFormat minuteDateFormatter;
    private SimpleDateFormat dateFormatter;
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    private CassandraConnectionProvider connectionProvider;
    
    private EventDetailDAOCassandraImpl eventDetailDao;
    
    private TimelineDAOCassandraImpl timelineDao;
    
    private DimEventsDAOCassandraImpl eventNameDao;
    
    private DimDateDAOCassandraImpl dimDate;
    
    private DimTimeDAOCassandraImpl dimTime;
    
    private AggregateDAOCassandraImpl stagingAgg;
    
    private DimUserDAOCassandraImpl dimUser;
    
    private DimResourceDAOImpl dimResource;
    
    private APIDAOCassandraImpl apiDao;
    
    private JobConfigSettingsDAOCassandraImpl configSettings;
    
    private RecentViewedResourcesDAOImpl recentViewedResources;
    
    private KafkaLogProducer kafkaLogWriter;
  
    private MicroAggregatorDAOmpl liveAggregator;
    
    private LiveDashBoardDAOImpl liveDashBoardDAOImpl;

    private ActivityStreamDaoCassandraImpl activityStreamDao;

    private RealTimeOperationConfigDAOImpl realTimeOperation;
    
    public static  Map<String,String> realTimeOperators;
    
    private MicroAggregatorProducer microAggregator;
    
    private static GeoLocation geo;
    
    public Collection<String> pushingEvents ;
    
    public Collection<String> statKeys ;
    
    public ColumnList<String> statMetrics ;
    
    public String viewEvents ;
    
    private Gson gson = new Gson();
    
    private String atmosphereEndPoint;
    
    private String VIEW_COUNT_REST_API_END_POINT;
    
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
        this.gson = new Gson();

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
     * Initialize Coulumn Family
     */
    
    private void init(Map<String, String> configOptionsMap) {
    	
        this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
        this.dateFormatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        
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
        this.liveAggregator = new MicroAggregatorDAOmpl(getConnectionProvider());
        this.liveDashBoardDAOImpl = new LiveDashBoardDAOImpl(getConnectionProvider());
        this.recentViewedResources = new RecentViewedResourcesDAOImpl(getConnectionProvider());
        this.activityStreamDao = new ActivityStreamDaoCassandraImpl(getConnectionProvider());
        this.realTimeOperation = new RealTimeOperationConfigDAOImpl(getConnectionProvider());
        this.dimResource = new DimResourceDAOImpl(getConnectionProvider());
        baseDao = new BaseCassandraRepoImpl(getConnectionProvider());
        //realTimeOperators = realTimeOperation.getOperators();
        Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily());
        realTimeOperators = new LinkedHashMap<String, String>();
        for (Row<String, String> row : operators) {
        	realTimeOperators.put(row.getKey(), row.getColumns().getStringValue("aggregator_json", null));
		}
        geo = new GeoLocation();
        pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "default~key").getColumnNames();
        viewEvents = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~events", DEFAULTCOLUMN).getStringValue();
        statMetrics =baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat~metrics");
        statKeys = statMetrics.getColumnNames();
        atmosphereEndPoint = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "atmosphere.end.point", DEFAULTCOLUMN).getStringValue();
        VIEW_COUNT_REST_API_END_POINT = atmosphereEndPoint = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN).getStringValue();
        
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
         
	         ColumnList<String> existingEventRecord = baseDao.readWithKey(ColumnFamily.DIMEVENTS.getColumnFamily(),eventData.getEventName());
	
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
    	Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObject);
    	
    	eventMap = this.formatEventMap(eventObject, eventMap);
    	
    	/*String existingEventRecord = eventNameDao.getEventId(eventMap.get("eventName"));*/
    	Column<String> eventId = baseDao.readWithKeyColumn(ColumnFamily.DIMEVENTS.getColumnFamily(), eventMap.get("eventName"), "event_id");
    	String existingEventRecord = eventId.getStringValue();

    	if(existingEventRecord == null || existingEventRecord.isEmpty()){
			 eventNameDao.saveEventName(eventObject.getEventName());
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
			logger.info("CORE: Writing to activity log - :"+ eventObject.getFields().toString());
			try {
				kafkaLogWriter.sendEventLog(eventObject.getFields());
			} catch(Exception e) {
				logger.info("Exception in kafka log writer send event");
			}
		}
   

		// Insert into event_timeline column family
		Date eventDateTime = new Date(eventObject.getStartTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();

		if(eventObject.getEventType() == null || !eventObject.getEventType().equalsIgnoreCase("stop") || !eventObject.getEventType().equalsIgnoreCase("completed-event")){
		    timelineDao.updateTimelineObject(eventObject, eventRowKey,eventKeyUUID.toString());
		}
		
		//To be revoked
		EventData eventData= getAndSetEventData(eventMap);
		this.updateEvent(eventData); 
		
		String aggregatorJson = realTimeOperators.get(eventMap.get("eventName"));
		
		if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){		 	

			try {
				liveAggregator.realTimeMetrics(eventMap, aggregatorJson);
			} catch(Exception e) {
				logger.info("Exception in real time metrics");
			}

			try {
				microAggregator.sendEventForAggregation(eventObject.getFields());
			} catch(Exception e) {
				logger.info("Exception in micro aggregator");
			}
		}
	  
		if(aggregatorJson != null && !aggregatorJson.isEmpty() && aggregatorJson.equalsIgnoreCase(RAWUPDATE)){
			liveAggregator.updateRawData(eventMap);
		}
		
		
		try {
			liveDashBoardDAOImpl.findDifferenceInCount(eventMap);
		} catch (ParseException e) {
			logger.info("Exception while finding difference : {} ",e);
		}
		
		liveDashBoardDAOImpl.callCountersV2(eventMap);
		
		liveDashBoardDAOImpl.addApplicationSession(eventMap);

		this.saveGeoLocations(eventMap);		

		if(pushingEvents.contains(eventMap.get("eventName"))){
			liveDashBoardDAOImpl.pushEventForAtmosphere(atmosphereEndPoint,eventMap);
		}

		if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
			liveDashBoardDAOImpl.pushEventForAtmosphereProgress(atmosphereEndPoint, eventMap);
		}

		if(viewEvents.contains(eventMap.get("eventName"))){
			liveDashBoardDAOImpl.addContentForPostViews(eventMap);
		}
		
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
        		eventDetailDao.updateParentId(eventData.getParentEventId(), endTime, (endTime-parentStartTime));
        	}
        }

    }
    
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
        		eventDetailDao.updateParentId(eventObject.getParentEventId(), endTime, (endTime-parentStartTime));
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
    	ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(),eventData.getApiKey());
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
    	//jobsCount = configSettings.getJobsCount();
    	jobsCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs_count").getLongValue();
    	if(jobsCount == 0 ){
    		jobsCount ++;
    		configSettings.balancingJobsCount(jobsCount);
    		baseDao.deleteAll(ColumnFamily.STAGING.getColumnFamily());
    		logger.info("Staging table truncated");
    	}else{
    		jobsCount ++;
    		configSettings.balancingJobsCount(jobsCount);
    		logger.info("Job is already running! so Staging table will not truncate");
    	}
    	
    	//Get all the event name and store for Caching
    	Map<String, String> events = new HashMap<String, String>();
    	Rows<String, String> eventRows = 	baseDao.readAllRows(ColumnFamily.DIMEVENTS.getColumnFamily());
    	for (Row<String, String> eventRow : eventRows) {
			events.put(eventRow.getKey(), eventRow.getColumns().getStringValue("event_id", null));
		}
    	//Process records for every minute
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		int currentHour = dateFormatter.parse(startDate.toString()).getHours();
    		int currentMinute = dateFormatter.parse(startDate.toString()).getMinutes();
    		
    		logger.info("Porcessing Date : {}" , startDate.toString());
    		
    		//Get Time ID and Hour ID
    		Map<String,String> timeDetailMap = new LinkedHashMap<String, String>();
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
   		 	
   		 	//Generate Key if loads custom Event Name
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
    	
    	//jobsCount = configSettings.getJobsCount();
    	jobsCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "running-jobs-count", "jobs_count").getLongValue();
    	jobsCount--;
    	configSettings.balancingJobsCount(jobsCount);
    	logger.info("Process Ends  : Inserted successfully");
    }

    public void postMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views_job_settings");
    	ColumnList<String> jobIds = baseDao.readWithKey(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), "job_ids");
    	//ColumnList<String> jobIds = recentViewedResources.getColumnList("job_ids");
    	
    	
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
    		
    		recentViewedResources.updateOrAddRow(jobId, "start_count", ""+startVal);
    		recentViewedResources.updateOrAddRow(jobId, "end_count", ""+endVal);
    		recentViewedResources.updateOrAddRow(jobId, "job_status", "Inprogress");
    		configSettings.updateOrAddRow("views_job_settings", "total_job_count", ""+totalJobCount);
    		configSettings.updateOrAddRow("views_job_settings", "running_job_count", ""+jobCount);
    		configSettings.updateOrAddRow("views_job_settings", "indexed_count", ""+endVal);
    		recentViewedResources.updateOrAddRow("job_ids", "job_names", runningJobs+","+jobId);
    		
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
    					liveDashBoardDAOImpl.generateCounter("all~"+columns.getColumnByName("gooru_oid").getStringValue(), "count~views", columns.getColumnByName("views_count").getLongValue(), m);
    					recentViewedResources.generateRow("all~"+columns.getColumnByName("gooru_oid").getStringValue(), "status", "migrated", m);
    					recentViewedResources.generateRow("all~"+columns.getColumnByName("gooru_oid").getStringValue(), "last_migrated", dateFormatter.format((new Date())).toString(), m);
    					recentViewedResources.generateRow("all~"+columns.getColumnByName("gooru_oid").getStringValue(), "last_updated", columns.getColumnByName("last_modified").getStringValue(), m);
    					recentViewedResources.generateRow("views~"+i, "gooruOid", columns.getColumnByName("gooru_oid").getStringValue(), m);
    					
    					//baseDao.generateNonCounter(cfName, key, columnName, value, m);
    				}
    			
    		}
    			m.execute();
    			long stop = System.currentTimeMillis();
    			/*recentViewedResources.updateOrAddRow(jobId, "job_status", "Completed");
    			recentViewedResources.updateOrAddRow(jobId, "run_time", (stop-start)+" ms");
    			configSettings.updateOrAddRow("views_job_settings", "total_time", ""+(totalTime + (stop-start)));
    			configSettings.updateOrAddRow("views_job_settings", "running_job_count", ""+(jobCount - 1));*/
    			
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
    	
    	//ColumnList<String> settings = configSettings.getColumnList("stat_job_settings");
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
    		
    		/*recentViewedResources.updateOrAddRow(jobId, "start_count", ""+startVal);
    		recentViewedResources.updateOrAddRow(jobId, "end_count", ""+endVal);
    		recentViewedResources.updateOrAddRow(jobId, "job_status", "Inprogress");
    		configSettings.updateOrAddRow("stat_job_settings", "total_job_count", ""+totalJobCount);
    		configSettings.updateOrAddRow("stat_job_settings", "running_job_count", ""+jobCount);
    		configSettings.updateOrAddRow("stat_job_settings", "indexed_count", ""+endVal);
    		recentViewedResources.updateOrAddRow("stat_job_ids", "job_names", runningJobs+","+jobId);*/
    		
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
	    			/*Column<String> gooruOidColumnString = baseDao.readWithKeyColumn(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"views~"+i, "gooruOid");
	    			if(gooruOidColumnString != null){
	    				gooruOid = gooruOidColumnString.getStringValue();
	    			}*/
	    			gooruOid = recentViewedResources.read("views~"+i, "gooruOid");
	    			
	    				logger.info("gooruOid : {}",gooruOid);
	    				if(gooruOid != null){
	    					OperationResult<ColumnList<String>>  vluesList = liveDashBoardDAOImpl.readLiveDashBoard("all~"+gooruOid, columnList);
	    					JSONObject resourceObj = new JSONObject();
	    					for(Column<String> detail : vluesList.getResult()) {
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
	    			/*recentViewedResources.updateOrAddRow(jobId, "job_status", "Completed");
	    			recentViewedResources.updateOrAddRow(jobId, "run_time", (stop-start)+" ms");
	    			configSettings.updateOrAddRow("stat_job_settings", "total_time", ""+(totalTime + (stop-start)));
	    			configSettings.updateOrAddRow("stat_job_settings", "running_job_count", ""+(jobCount - 1));*/
	    			
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
				logger.info("gooruOids : {} ",id);
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
				logger.info("balancedView : {} ",balancedView);
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
				logger.info("resourceList : {} ",resourceList);
				if(resourceList.length() != 0){
					this.callStatAPI(resourceList, null);
				}
				
				configSettings.updateOrAddRow("bal_stat_job_settings", "last_updated_time", String.valueOf(startDate));
				
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
    		
    		Map<String,String> timeDetailMap = new LinkedHashMap<String, String>();
    		timeDetailMap.put("hour", String.valueOf(currentHour));
    		timeDetailMap.put("minute", String.valueOf(currentMinute));
    		Rows<String, String> timeDetails =  baseDao.readIndexedColumnList(ColumnFamily.DIMTIME.getColumnFamily(),timeDetailMap);
    		
	   		 for(Row<String, String> timeIds : timeDetails){
	   			 minuteId = timeIds.getColumns().getStringValue("time_hm_id", null);
				 hourId = timeIds.getColumns().getLongValue("time_h_id", null).toString();
			 }

	   		 ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), startDate.toString());
	    	
	    	if(eventUUID == null && eventUUID.isEmpty() ) {
	    		logger.info("No events in given timeline :  {}",startDate);
	    		return;
	    	}

	    	for(int i = 0 ; i < eventUUID.size() ; i++) {
	    		
	    		UUID stagingKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
	    		ColumnList<String> eventDetails = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailUUID);
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
    	liveAggregator.updateCounter(gooruoid,LoaderConstants.VIEWS.getName(), viewcount );
    }
    
    public void addAggregators(String eventName, String json ,String updateBy) {
    	realTimeOperation.addAggregators(eventName,json, updateBy);
    }
    /**
     *  Update bulk view count
     * @throws JSONException 
     */
    public void callAPIViewCount() throws JSONException {
    	JSONArray resourceList = new JSONArray();
    	//String lastUpadatedTime = configSettings.getConstants("views~last~updated", DEFAULTCOLUMN);
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
		if(!currentTime.equals(minuteDateFormatter.format(rowValues)) && (rowValues.getTime() < currDate.getTime())){
		ColumnList<String> contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+minuteDateFormatter.format(rowValues));		
		logger.info("stat-mig key : {} ",VIEWS+SEPERATOR+minuteDateFormatter.format(rowValues));
		for(int i = 0 ; i < contents.size() ; i++) {
			OperationResult<ColumnList<String>>  vluesList = liveDashBoardDAOImpl.readLiveDashBoard("all~"+contents.getColumnByIndex(i).getName(), statKeys);
			JSONObject resourceObj = new JSONObject();
			for(Column<String> detail : vluesList.getResult()) {
				resourceObj.put("gooruOid", contents.getColumnByIndex(i).getStringValue());
				for(String column : statKeys){
					if(detail.getName().equals(column)){
						logger.info("statValuess : {}",statMetrics.getStringValue(column, null));
						resourceObj.put(statMetrics.getStringValue(column, null), detail.getLongValue());
						resourceObj.put("resourceType", "resource");
					}
				}
			}
			logger.info("gooruOid : {}" , contents.getColumnByIndex(i).getStringValue());
			resourceList.put(resourceObj);
		}
		
		if((resourceList.length() != 0)){
			this.callStatAPI(resourceList, rowValues);
		}else{
			configSettings.updateOrAddRow("views~last~updated", DEFAULTCOLUMN, minuteDateFormatter.format(rowValues));
 	 		logger.info("No content viewed");
		}
	 }
   }
  
    private void callStatAPI(JSONArray resourceList,Date rowValues){
    	JSONObject staticsObj = new JSONObject();
		//String sessionToken = configSettings.getConstants(LoaderConstants.SESSIONTOKEN.getName(),DEFAULTCOLUMN);
		String sessionToken = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN).getStringValue();
		try{
				String url = VIEW_COUNT_REST_API_END_POINT + "?skipReindex=true&sessionToken=" + sessionToken;
				DefaultHttpClient httpClient = new DefaultHttpClient();   
				staticsObj.put("statisticsData", resourceList);
				StringEntity input = new StringEntity(staticsObj.toString());			        
		 		HttpPost  postRequest = new HttpPost(url);
		 		postRequest.addHeader("accept", "application/json");
		 		postRequest.setEntity(input);
		 		HttpResponse response = httpClient.execute(postRequest);
		 		logger.info("Status : {} ",response.getStatusLine().getStatusCode());
		 		if (response.getStatusLine().getStatusCode() != 200) {
		 	 		logger.info("View count api call failed...");
		 	 		throw new AccessDeniedException("Something went wrong! Api fails");
		 		} else {
		 			if(rowValues != null){
		 				configSettings.updateOrAddRow("views~last~updated", DEFAULTCOLUMN, minuteDateFormatter.format(rowValues));
		 			}
		 	 		logger.info("View count api call Success...");
		 		}
		 			
		} catch(Exception e){
			e.printStackTrace();
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
    
    public Map<String,String> createEvent(String eventName){
    	Map<String,String> status = new HashMap<String, String>();
    	try {
			if(baseDao.isRowKeyExists(ColumnFamily.DIMEVENTS.getColumnFamily(),eventName)){
				if(eventNameDao.saveEventName(eventName)){
					status.put("status", eventName+" is Created ");
					return status;
				}
			}else{
				status.put("status", "Event Name already Exists");
				return status;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	status.put("status", "unable to a create this event "+eventName);
    	return status;
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

    private void saveGeoLocations(Map<String,String> eventMap) throws IOException{
    	
		if(eventMap.containsKey("userIp") && eventMap.get("userIp") != null && !eventMap.get("userIp").isEmpty()){
			
			GeoData geoData = new GeoData();
			
			CityResponse res = geo.getGeoResponse(eventMap.get("userIp"));			

			if(res != null && res.getCountry().getName() != null){
				geoData.setCountry(res.getCountry().getName());
				eventMap.put("country", res.getCountry().getName());
			}
			if(res != null && res.getCity().getName() != null){
				geoData.setCity(res.getCity().getName());
				eventMap.put("city", res.getCity().getName());
			}
			if(res != null && res.getLocation().getLatitude() != null){
				geoData.setLatitude(res.getLocation().getLatitude());
			}
			if(res != null && res.getLocation().getLongitude() != null){
				geoData.setLongitude(res.getLocation().getLongitude());
			}
			if(res != null && res.getMostSpecificSubdivision().getName() != null){
				geoData.setState(res.getMostSpecificSubdivision().getName());
				eventMap.put("state", res.getMostSpecificSubdivision().getName());
			}
			
			if(geoData.getLatitude() != null && geoData.getLongitude() != null){
				liveDashBoardDAOImpl.saveGeoLocation(geoData);
			}			
		}
    }

    
    /**
     * @param connectionProvider the connectionProvider to set
     */
    public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
    	this.connectionProvider = connectionProvider;
    }
    
}

  
