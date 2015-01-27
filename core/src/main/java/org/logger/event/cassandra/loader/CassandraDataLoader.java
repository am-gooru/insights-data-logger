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
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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
import com.netflix.astyanax.ColumnListMutation;
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

    private static final Logger activityErrorLog = LoggerFactory.getLogger("activityErrorLog");

    private static final Logger activityLogger = LoggerFactory.getLogger("activityLog");
    
    private Keyspace cassandraKeyspace;
    
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
    
    private static final ConsistencyLevel WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
   
    private SimpleDateFormat minuteDateFormatter;
    
    private SimpleDateFormat dateFormatter;
    
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    
    private CassandraConnectionProvider connectionProvider;
    
    private KafkaLogProducer kafkaLogWriter;
  
    private MicroAggregatorDAOmpl liveAggregator;
    
    private LiveDashBoardDAOImpl liveDashBoardDAOImpl;
    
    public static  Map<String,String> cache;
    
    public static  Map<String,String> resourceCodeType;
    
    public static  Map<String,Object> licenseCache;
    
    public static  Map<String,Object> resourceTypesCache;
    
    public static  Map<String,Object> categoryCache;
    
    public static  Map<String,Object> gooruTaxonomy;
    
    private MicroAggregatorProducer microAggregator;
    
    private static GeoLocation geo;
    
    public Collection<String> pushingEvents ;
    
    public Collection<String> statKeys ;
    
    public ColumnList<String> statMetrics ;
        
    private BaseCassandraRepoImpl baseDao ;
    
    private String configuredIp;
    
    private String KafkaTopic;

    private HashMap<String, Map<String, String>> tablesDataTypeCache;
    
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
        KafkaTopic = KAFKA_FILE_TOPIC;
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
        KafkaTopic = KAFKA_FILE_TOPIC;
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

        Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(),0);
        cache = new LinkedHashMap<String, String>();
        for (Row<String, String> row : operators) {
        	cache.put(row.getKey(), row.getColumns().getStringValue("aggregator_json", null));
		}
        cache.put(VIEWEVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~events", DEFAULTCOLUMN,0).getStringValue());
        cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "atmosphere.end.point", DEFAULTCOLUMN,0).getStringValue());
        cache.put(VIEWUPDATEENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN,0).getStringValue());
        cache.put(SESSIONTOKEN, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN,0).getStringValue());
        cache.put(SEARCHINDEXAPI, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULTCOLUMN,0).getStringValue());
        geo = new GeoLocation();
        
        ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "schdulers~status",0);
        for(int i = 0 ; i < schdulersStatus.size() ; i++) {
        	cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
        }
        pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "default~key",0).getColumnNames();
        statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat~metrics",0);
        statKeys = statMetrics.getColumnNames();
        
        resourceCodeType = new LinkedHashMap<String, String>();
        
        ColumnList<String> resourceCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), ColumnFamily.DIMRESOURCE.getColumnFamily(),0);
        for(int i = 0 ; i < resourceCodeTypeList.size() ; i++) {
                resourceCodeType.put(resourceCodeTypeList.getColumnByIndex(i).getName(), resourceCodeTypeList.getColumnByIndex(i).getStringValue());
        }
        
        Rows<String, String> licenseRows = baseDao.readAllRows(ColumnFamily.LICENSE.getColumnFamily(),0);
        licenseCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : licenseRows) {
        	licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        Rows<String, String> resourceTypesRows = baseDao.readAllRows(ColumnFamily.RESOURCETYPES.getColumnFamily(),0);
        resourceTypesCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : resourceTypesRows) {
        	resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        Rows<String, String> categoryRows = baseDao.readAllRows(ColumnFamily.CATEGORY.getColumnFamily(),0);
        categoryCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : categoryRows) {
        	categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
       
        configuredIp = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"schedular~ip",0).getStringValue("ip_address", null);
        
        tablesDataTypeCache = new HashMap<String, Map<String,String>>();
        Rows<String, String> tableDataTypeRows = baseDao.readAllRows(ColumnFamily.TABLE_DATATYPE.getColumnFamily(),0);

        for (Row<String, String> row : tableDataTypeRows) {
        	Map<String,String> columnMap = new HashMap<String, String>();
        	for(Column<String> column : row.getColumns()){
        		columnMap.put(column.getName().trim(), column.getStringValue());
        	}
        	tablesDataTypeCache.put(row.getKey(),columnMap);
		}
        
    }

    public void clearCache(){
    	cache.clear();
    	Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(),0);
        cache = new LinkedHashMap<String, String>();
        for (Row<String, String> row : operators) {
        	cache.put(row.getKey(), row.getColumns().getStringValue("aggregator_json", null));
		}

        cache.put(VIEWEVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "views~events", DEFAULTCOLUMN,0).getStringValue());
        cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "atmosphere.end.point", DEFAULTCOLUMN,0).getStringValue());
        cache.put(VIEWUPDATEENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULTCOLUMN,0).getStringValue());
        cache.put(SESSIONTOKEN, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN,0).getStringValue());
        cache.put(SEARCHINDEXAPI, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULTCOLUMN,0).getStringValue());
        
        pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "default~key",0).getColumnNames();
        statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat~metrics",0);
        
        statKeys = statMetrics.getColumnNames();
        liveDashBoardDAOImpl.clearCache();
        ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "schdulers~status",0);
        for(int i = 0 ; i < schdulersStatus.size() ; i++) {
        	cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
        }
        
        Rows<String, String> licenseRows = baseDao.readAllRows(ColumnFamily.LICENSE.getColumnFamily(),0);
        licenseCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : licenseRows) {
        	licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        Rows<String, String> resourceTypesRows = baseDao.readAllRows(ColumnFamily.RESOURCETYPES.getColumnFamily(),0);
        resourceTypesCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : resourceTypesRows) {
        	resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        Rows<String, String> categoryRows = baseDao.readAllRows(ColumnFamily.CATEGORY.getColumnFamily(),0);
        categoryCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : categoryRows) {
        	categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        configuredIp = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"schedular~ip",0).getStringValue("ip_address", null);
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
	        	 existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getEventId(),0);
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
				kafkaLogWriter.sendEventLog(eventData.getFields(),KafkaTopic);
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
    	Map<String,String> eventMap = new LinkedHashMap<String, String>();
    	String aggregatorJson = null;
    	
    	try {
	    	eventMap = JSONDeserializer.deserializeEventObject(eventObject);    	

	    	eventMap = this.formatEventMap(eventObject, eventMap);
									
			aggregatorJson = cache.get(eventMap.get("eventName"));
			
			if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){		 	
				liveAggregator.realTimeMetrics(eventMap, aggregatorJson);	
			}
			
			if(String.valueOf(eventMap.get("eventName")).equalsIgnoreCase("collection.play") ||
                    String.valueOf(eventMap.get("eventName")).equalsIgnoreCase("resource.play") ||
                    String.valueOf(eventMap.get("eventName")).equalsIgnoreCase("collection.resource.play")){
                            if(String.valueOf(eventMap.get(CONTENTGOORUOID)) != null){
                            	balanceLiveBoardData(eventMap.get(CONTENTGOORUOID));
                            }
            }
    	}catch(Exception e){
			logger.info("Writing error log : {} ",eventObject.getEventId());
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

			ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventObject.getEventId(),0);
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
        	ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventObject.getParentEventId(),0);
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
    	
    	String dateId = null;
    	Long weekId = null;
    	Long monthId = null;
    	String eventId = null;
    	Long yearId = null;
    	String processingDate = null;
    	
    	//Get all the event name and store for Caching
    	Map<String,String> events = new LinkedHashMap<String, String>();
    	
    	Rows<String, String> eventRows  = baseDao.readAllRows(ColumnFamily.DIMEVENTS.getColumnFamily(),0);
    	
    	for(Row<String, String> eventRow : eventRows){
    		ColumnList<String> eventColumns = eventRow.getColumns();
    		events.put(eventColumns.getStringValue("event_name", null), eventColumns.getStringValue("event_id", null));
    	}
    	//Process records for every minute
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		
    		logger.info("Porcessing Date : {}" , startDate.toString());
    		
   		 	if(!currentDate.equalsIgnoreCase(processingDate)){
   		 			processingDate = currentDate;
   		 		Rows<String, String> dateDetail = baseDao.readIndexedColumn(ColumnFamily.DIMDATE.getColumnFamily(),"date",currentDate,0);
   		 			
   		 		for(Row<String, String> dateIds : dateDetail){
   		 			ColumnList<String> columns = dateIds.getColumns();
   		 			dateId = dateIds.getKey().toString();
   		 			monthId = columns.getLongValue("month_date_id", 0L);
   		 			weekId = columns.getLongValue("week_date_id", 0L);
   		 			yearId = columns.getLongValue("year_date_id", 0L);
   		 		}	
   		 	}
   		 	
   		 	//Retry 100 times to get Date ID if Cassandra failed to respond
   		 	int dateTrySeq = 1;
   		 	while((dateId == null || dateId.equalsIgnoreCase("0")) && dateTrySeq < 100){
   		 	
   		 		Rows<String, String> dateDetail = baseDao.readIndexedColumn(ColumnFamily.DIMDATE.getColumnFamily(),"date",currentDate,0);
	 			
		 		for(Row<String, String> dateIds : dateDetail){
		 			ColumnList<String> columns = dateIds.getColumns();
   		 			dateId = dateIds.getKey().toString();
   		 			monthId = columns.getLongValue("month_date_id", 0L);
   		 			weekId = columns.getLongValue("week_date_id", 0L);
   		 			yearId = columns.getLongValue("year_date_id", 0L);
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
   		 ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey,0);
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
	    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailkeys,0);

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
	    		
	    		//Get Event ID for corresponding Event Name
	    		 eventId = events.get(searchType);
	    		 
	    		
	    		if(eventId == null) {
	    			continue;
	    		}
		    	
	    		String fields = row.getColumns().getStringValue("fields", null);
	    		if(fields != null){
	    		try {
	    			JSONObject jsonField = new JSONObject(fields);
		    			if(jsonField.has("version")){
		    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
		    				Map<String, Object> eventMap = JSONDeserializer.deserializeEventObject(eventObjects);    	
		    				
		    				eventMap.put("eventName", eventObjects.getEventName());
		    		    	eventMap.put("eventId", eventObjects.getEventId());
		    		    	eventMap.put("eventTime",String.valueOf(eventObjects.getStartTime()));
		    		    	if(eventMap.get(CONTENTGOORUOID) != null){		    		    		
		    		    		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
		    		    		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
		    		    	}
		    		    	if(eventMap.get(GOORUID) != null){  
		    		    		eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
		    		    	}
		    		    	eventMap.put("dateId", dateId);
		    		    	eventMap.put("weekId", weekId);
		    		    	eventMap.put("monthId", monthId);
		    		    	eventMap.put("yearId", yearId);
		    		    	
		    		    	liveDashBoardDAOImpl.saveInStaging(eventMap);
		    			} 
		    			else{
		    				   Iterator<?> keys = jsonField.keys();
		    				   Map<String,Object> eventMap = new HashMap<String, Object>();
		    				   while( keys.hasNext() ){
		    			            String key = (String)keys.next();
		    			            if(key.equalsIgnoreCase("contentGooruId") || key.equalsIgnoreCase("gooruOId") || key.equalsIgnoreCase("gooruOid")){
		    			            	eventMap.put(CONTENTGOORUOID, String.valueOf(jsonField.get(key)));
		    			            }

		    			            if(key.equalsIgnoreCase("gooruUId") || key.equalsIgnoreCase("gooruUid")){
		    			            	eventMap.put(GOORUID, String.valueOf(jsonField.get(key)));
		    			            }
		    			            eventMap.put(key,String.valueOf(jsonField.get(key)));
		    			        }
		    				   if(eventMap.get(CONTENTGOORUOID) != null){
		    				   		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
		    				   		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
		    				   }
		    				   if(eventMap.get(GOORUID) != null){
		    					   eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
		    				   }
		    				   	eventMap.put("dateId", dateId);
			    		    	eventMap.put("weekId", weekId);
			    		    	eventMap.put("monthId", monthId);
			    		    	eventMap.put("yearId", yearId);	
			    	    		liveDashBoardDAOImpl.saveInStaging(eventMap);
		    		     }
					} catch (Exception e) {
						logger.info("Error while Migration : {} ",e);
					}
	    			}	    		
	    		}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
    	}
    	

    	logger.info("Process Ends  : Inserted successfully");
    }
    
    public void updateStagingES(String startTime , String endTime,String customEventName,String apiKey) throws ParseException {
    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    	SimpleDateFormat dateIdFormatter = new SimpleDateFormat("yyyy-MM-dd 00:00:00+0000");
    	Calendar cal = Calendar.getInstance();
    	for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
    		String currentDate = dateIdFormatter.format(dateFormatter.parse(startDate.toString()));
    		int currentHour = dateFormatter.parse(startDate.toString()).getHours();
    		int currentMinute = dateFormatter.parse(startDate.toString()).getMinutes();
    		
    		logger.info("Porcessing Date : {}" , startDate.toString());
   		 	String timeLineKey = null;   		 	
   		 	if(customEventName == null || customEventName  == "") {
   		 		timeLineKey = startDate.toString();
   		 	} else {
   		 		timeLineKey = startDate.toString()+"~"+customEventName;
   		 	}
   		 	
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey,0);
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
	    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailkeys,0);
	    	
	    	
	    	for (Row<String, String> row : eventDetailsNew) {
	    		this.saveActivityInIndex(row.getColumns().getStringValue("fields", null));
	    	}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
	    	}
	    
    }
    public void migrateFirstSessionData(String startTime , String endTime) throws ParseException {
    	
   	 for (Long startDate = minuteDateFormatter.parse(startTime).getTime() ; startDate < minuteDateFormatter.parse(endTime).getTime();) {

		   String currentDate = minuteDateFormatter.format(new Date(startDate));
          logger.info("Processing Date : {}" , currentDate);
 		 	String timeLineKey = null;   		 	
 		 	
 		 	timeLineKey = currentDate.toString()+"~collection.play";
 		 	
 		 	
 		 	//Read Event Time Line for event keys and create as a Collection
 		 	ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey,0);
	    	if(eventUUID == null && eventUUID.isEmpty() ) {
	    		logger.info("No events in given timeline :  {}",startDate);
	    		return;
	    	}
	 
	    	try{
		    	for(int i = 0 ; i < eventUUID.size() ; i++) {
		    	String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
		    	
		    	logger.info("Event Id " + eventDetailUUID) ;
		    	
		    	ColumnList<String> eventDetailsRow = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailUUID,0);
		    	
		    if(eventDetailsRow != null && !eventDetailsRow.isEmpty()) {
		    	String fields = eventDetailsRow.getStringValue("fields", null);
		    	JSONObject jsonField = new JSONObject(fields);
    			if(jsonField.has("version")){
    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
    				Map<String, String> eventMap = JSONDeserializer.deserializeEventObject(eventObjects);   
    				
    				if(!eventMap.get(GOORUID).equals("ANONYMOUS") && eventMap.containsKey(PARENTGOORUOID) && StringUtils.isNotBlank(eventMap.get(PARENTGOORUOID))){
    					String keyPart = eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID);
    					logger.info("keyPart:"+keyPart);
    					long columnCount = baseDao.getCount(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), "FS~"+keyPart);
    					logger.info("columnCount:"+columnCount);
    					if( columnCount == 0L){
    						ColumnList<String> sessions = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), keyPart,0);
    						String firstSessionId = sessions.getColumnByIndex(0).getName();
    						
    						logger.info("classpage id: "+ eventMap.get(PARENTGOORUOID));
    
    						boolean isStudent = baseDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),eventMap.get(PARENTGOORUOID),0);
    						
    						logger.info("Key to read : " + (firstSessionId+"~"+keyPart));
    					
    						logger.info("isStudent : " + isStudent);
    						if(isStudent){
    							MutationBatch m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
    							ColumnList<String> sessionData = baseDao.readWithKey(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), (firstSessionId+"~"+keyPart),0);
    							for(int j = 0 ; j < sessionData.size() ; j++ ){
    								
    								if ((sessionData.getColumnByIndex(i).getName().endsWith("~grade_in_percentage") || sessionData.getColumnByIndex(i).getName().endsWith("~question_count") || sessionData.getColumnByIndex(i).getName().endsWith("~views")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~avg_time_spent") || sessionData.getColumnByIndex(i).getName().endsWith("~time_spent") || sessionData.getColumnByIndex(i).getName().endsWith("~A") || sessionData.getColumnByIndex(i).getName().endsWith("~B")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("is_group_owner") || sessionData.getColumnByIndex(i).getName().endsWith("question_id") || sessionData.getColumnByIndex(i).getName().endsWith("~C") || sessionData.getColumnByIndex(i).getName().endsWith("~D")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~E") || sessionData.getColumnByIndex(i).getName().endsWith("~F") || sessionData.getColumnByIndex(i).getName().endsWith("answer_id") || sessionData.getColumnByIndex(i).getName().endsWith("sequence") || sessionData.getColumnByIndex(i).getName().endsWith("is_correct")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~skipped") || sessionData.getColumnByIndex(i).getName().endsWith("deleted") || sessionData.getColumnByIndex(i).getName().endsWith("active_flag")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~avg_reaction") || sessionData.getColumnByIndex(i).getName().endsWith("~RA") || sessionData.getColumnByIndex(i).getName().endsWith("~feed_back_timestamp") || sessionData.getColumnByIndex(i).getName().endsWith("~skipped~status")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~feed_back_time_spent")  || sessionData.getColumnByIndex(i).getName().endsWith("is_required") || sessionData.getColumnByIndex(i).getName().endsWith("~question_status") || sessionData.getColumnByIndex(i).getName().endsWith("~score")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~tau") || sessionData.getColumnByIndex(i).getName().endsWith("~in-correct") || sessionData.getColumnByIndex(i).getName().endsWith("~correct") || sessionData.getColumnByIndex(i).getName().endsWith("item_sequence")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("status") || sessionData.getColumnByIndex(i).getName().endsWith("item_sequence")
    										|| sessionData.getColumnByIndex(i).getName().endsWith("~attempts"))) {
    										baseDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily()+"_temp", "FS~"+keyPart, sessionData.getColumnByIndex(i).getName(), sessionData.getColumnByIndex(i).getLongValue(), m);
    								}else {
    									baseDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily()+"_temp", "FS~"+keyPart, sessionData.getColumnByIndex(i).getName(), sessionData.getColumnByIndex(i).getStringValue(), m);
    								}
    								
    							}
    							m.execute();
    						}
    					}
    				}
    			}
		    }		    
		    }
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    	//Incrementing time - one minute
          startDate = new Date(startDate).getTime() + 60000;
          
	    }
   	 
    }
    
    public void eventMigration(String startTime , String endTime,String customEventName,boolean isSchduler) throws ParseException {

    	if(isSchduler){
    		 baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~status", DEFAULTCOLUMN,"in-progress");
    	}
    	 for (Long startDate = minuteDateFormatter.parse(startTime).getTime() ; startDate < minuteDateFormatter.parse(endTime).getTime();) {

  		   String currentDate = minuteDateFormatter.format(new Date(startDate));
            logger.info("Processing Date : {}" , currentDate);
   		 	String timeLineKey = null;   		 	
   		 	if(customEventName == null || customEventName  == "") {
   		 		timeLineKey = currentDate.toString();
   		 	} else {
   		 		timeLineKey = currentDate.toString()+"~"+customEventName;
   		 	}
   		 	
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey,0);
	    	if(eventUUID == null && eventUUID.isEmpty() ) {
	    		logger.info("No events in given timeline :  {}",startDate);
	    		return;
	    	}
	 
	    	try{
		    	MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
		    	for(int i = 0 ; i < eventUUID.size() ; i++) {
		    	String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
		    	
		    	logger.info("Event Id " + eventDetailUUID) ;
		    	
		    	ColumnList<String> eventDetailsRow = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailUUID,0);
		    	
		    if(eventDetailsRow != null && eventDetailsRow.getColumnByName("event_name") != null) {
		    	
	    		for(int j = 0 ; j < eventDetailsRow.size() ; j++ ){
	    			if ((eventDetailsRow.getColumnByIndex(j).getName().equalsIgnoreCase("time_spent_in_millis") || eventDetailsRow.getColumnByIndex(j).getName().equalsIgnoreCase("start_time") || eventDetailsRow.getColumnByIndex(j).getName().equalsIgnoreCase("end_time"))) {
	    				baseDao.generateNonCounter(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailUUID, eventDetailsRow.getColumnByIndex(j).getName(), eventDetailsRow.getColumnByIndex(j).getLongValue(), m);
					}else {
						baseDao.generateNonCounter(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailUUID, eventDetailsRow.getColumnByIndex(j).getName(), eventDetailsRow.getColumnByIndex(j).getStringValue(), m);
					}
	    		}
	    		
	    		String aggregatorJson = null;
	    		if(eventDetailsRow.getColumnByName("event_name").getStringValue() != null){
	    			aggregatorJson = cache.get(eventDetailsRow.getColumnByName("event_name").getStringValue());
	    		}
	    		
	    		if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){		 	
	    			EventObject eventObjects = new Gson().fromJson(eventDetailsRow.getColumnByName("fields").getStringValue(), EventObject.class);
	    			this.handleEventObjectMessage(eventObjects);
	    		}

	    	 }
		    
		    }
		    	m.execute();
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    	//Incrementing time - one minute
            startDate = new Date(startDate).getTime() + 60000;
            
		    if(isSchduler){
		    	baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~last~updated", DEFAULTCOLUMN,""+minuteDateFormatter.format(new Date(startDate)));
		    }
	    }
    	if(isSchduler){
    		 baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~status", DEFAULTCOLUMN,"completed");
    	}
    }
    
    @Async
    public void saveActivityInIndex(String fields){
    	if(fields != null){
			try {
				JSONObject jsonField = new JSONObject(fields);
	    			if(jsonField.has("version")){
	    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
	    				Map<String,Object> eventMap = JSONDeserializer.deserializeEventObject(eventObjects);    	
	    				
	    				eventMap.put("eventName", eventObjects.getEventName());
	    		    	eventMap.put("eventId", eventObjects.getEventId());
	    		    	eventMap.put("eventTime",String.valueOf(eventObjects.getStartTime()));
	    		    	if(eventMap.get(CONTENTGOORUOID) != null){		    		    		
	    		    		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    		    		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    		    	}
	    		    	if(eventMap.get(GOORUID) != null){  
	    		    		eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
	    		    	}
	    	    		
	    	    		liveDashBoardDAOImpl.saveActivityInESIndex(eventMap,ESIndexices.EVENTLOGGER.getIndex(), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
	    			} 
	    			else{
	    				   Iterator<?> keys = jsonField.keys();
	    				   Map<String,Object> eventMap = new HashMap<String, Object>();
	    				   while( keys.hasNext() ){
	    			            String key = (String)keys.next();
	    			            if(key.equalsIgnoreCase("contentGooruId") || key.equalsIgnoreCase("gooruOId") || key.equalsIgnoreCase("gooruOid")){
	    			            	eventMap.put(CONTENTGOORUOID, String.valueOf(jsonField.get(key)));
	    			            }
	
	    			            if(key.equalsIgnoreCase("gooruUId") || key.equalsIgnoreCase("gooruUid")){
	    			            	eventMap.put(GOORUID, String.valueOf(jsonField.get(key)));
	    			            }
	    			            eventMap.put(key,String.valueOf(jsonField.get(key)));
	    			        }
	    				   if(eventMap.get(CONTENTGOORUOID) != null){
	    				   		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    				   		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    				   }
	    				   if(eventMap.get(GOORUID) != null ){
	    					   eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
	    				   }
		    	    		
		    	    		liveDashBoardDAOImpl.saveActivityInESIndex(eventMap,ESIndexices.EVENTLOGGER.getIndex(), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
	    		     }
				} catch (Exception e) {
					logger.info("Error while Migration : {} ",e);
				}
				}
		
    }
    
    public Map<String, Object> getUserInfo(Map<String,Object> eventMap , String gooruUId){
    	Collection<String> user = new ArrayList<String>();
    	user.add(gooruUId);
    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), user,0);
    	for (Row<String, String> row : eventDetailsNew) {
    		ColumnList<String> userInfo = row.getColumns();
    		for(int i = 0 ; i < userInfo.size() ; i++) {
    			String columnName = userInfo.getColumnByIndex(i).getName();
    			String value = userInfo.getColumnByIndex(i).getStringValue();
    			if(!columnName.equalsIgnoreCase("teacher") && !columnName.equalsIgnoreCase("organizationUId")){    				
    				eventMap.put(columnName, Long.valueOf(value));
    			}
    			if(value != null && columnName.equalsIgnoreCase("organizationUId")){
    				eventMap.put(columnName, value);
    			}
    			if(value != null && columnName.equalsIgnoreCase("teacher")){
    				JSONArray jArray = new JSONArray();
    				for(String val : value.split(",")){
    					jArray.put(val);
    				}
    				eventMap.put(columnName, jArray.toString());
    			}
    		}
    	}
		return eventMap;
    }
    public Map<String,Object> getContentInfo(Map<String,Object> eventMap,String gooruOId){
    	ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+gooruOId,0);
    		if(resource != null){
    			eventMap.put("title", resource.getStringValue("title", null));
    			eventMap.put("description",resource.getStringValue("description", null));
    			eventMap.put("sharing", resource.getStringValue("sharing", null));
    			eventMap.put("contentType", resource.getStringValue("category", null));
    			eventMap.put("license", resource.getStringValue("license_name", null));
    			
    			if(resource.getColumnByName("type_name") != null){
					if(resourceTypesCache.containsKey(resource.getColumnByName("type_name").getStringValue())){    							
						eventMap.put("resourceTypeId", resourceTypesCache.get(resource.getColumnByName("type_name").getStringValue()));
					}
				}
				if(resource.getColumnByName("category") != null){
					if(categoryCache.containsKey(resource.getColumnByName("category").getStringValue())){    							
						eventMap.put("resourceCategoryId", categoryCache.get(resource.getColumnByName("category").getStringValue()));
					}
				}
				ColumnList<String> questionCount = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), gooruOId,0);
				if(questionCount != null && !questionCount.isEmpty()){
					Long questionCounts = questionCount.getLongValue("questionCount", 0L);
					eventMap.put("questionCount", questionCounts);
					if(questionCounts > 0L){
						if(resourceTypesCache.containsKey(resource.getColumnByName("type_name").getStringValue())){    							
							eventMap.put("resourceTypeId", resourceTypesCache.get(resource.getColumnByName("type_name").getStringValue()));
						}	
					}
				}else{
					eventMap.put("questionCount",0L);
				}
    		} 
    	
		return eventMap;
    }
    public Map<String,Object> getTaxonomyInfo(Map<String,Object> eventMap,String gooruOid){
    	Collection<String> user = new ArrayList<String>();
    	user.add(gooruOid);
    	Map<String,String> whereColumn = new HashMap<String, String>();
    	whereColumn.put("gooru_oid", gooruOid);
    	Rows<String, String> eventDetailsNew = baseDao.readIndexedColumnList(ColumnFamily.DIMCONTENTCLASSIFICATION.getColumnFamily(), whereColumn,0);
    	Long subjectCode = 0L;
    	Long courseCode = 0L;
    	Long unitCode = 0L;
    	Long topicCode = 0L;
    	Long lessonCode = 0L;
    	Long conceptCode= 0L;
    	
    	JSONArray taxArray = new JSONArray();
    	for (Row<String, String> row : eventDetailsNew) {
    		ColumnList<String> userInfo = row.getColumns();
    			Long root = userInfo.getColumnByName("root_node_id") != null ? userInfo.getColumnByName("root_node_id").getLongValue() : 0L;
    			if(root == 20000L){
	    			Long value = userInfo.getColumnByName("code_id") != null ?userInfo.getColumnByName("code_id").getLongValue() : 0L;
	    			Long depth = userInfo.getColumnByName("depth") != null ?  userInfo.getColumnByName("depth").getLongValue() : 0L;
	    			if(value != null &&  depth == 1L){    				
	    				subjectCode = value;
	    			} 
	    			else if(value != null && depth == 2L){
	    			ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
	    			Long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
	    				if(subjectCode == 0L)
	    				subjectCode = subject;
	    				courseCode = value;
	    			}
	    			
	    			else if(value != null && depth == 3L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			Long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			Long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			if(subjectCode == 0L)
		    			subjectCode = subject;
		    			if(courseCode == 0L)
		    			courseCode = course;
		    			unitCode = value;
	    			}
	    			else if(value != null && depth == 4L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			Long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			Long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			Long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
		    			if(subjectCode == 0L)
			    		subjectCode = subject;
			    		if(courseCode == 0L)
			    		courseCode = course;
			    		if(unitCode == 0L)
			    		unitCode = unit;
			    		topicCode = value ;
	    			}
	    			else if(value != null && depth == 5L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			Long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			Long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			Long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
		    			Long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : 0L;
		    				if(subjectCode == 0L)
				    		subjectCode = subject;
				    		if(courseCode == 0L)
				    		courseCode = course;
				    		if(unitCode == 0L)
				    		unitCode = unit;
				    		if(topicCode == 0L)
				    		topicCode = topic ;
				    		lessonCode = value;
	    			}
	    			else if(value != null && depth == 6L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			Long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			Long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			Long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
		    			Long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : 0L;
		    			Long lesson = columns.getColumnByName("lesson_code_id") != null ? columns.getColumnByName("lesson_code_id").getLongValue() : 0L;
		    			if(subjectCode == 0L)
				    		subjectCode = subject;
				    		if(courseCode == 0L)
				    		courseCode = course;
				    		if(unitCode == 0L)
				    		unitCode = unit;
				    		if(topicCode == 0L)
				    		topicCode = topic ;
				    		if(lessonCode == 0L)
				    		lessonCode = lesson;
				    		conceptCode = value;
	    			}
	    			else if(value != null){
	    				taxArray.put(value);
	    				
	    			}
    		}else{
    			Long value = userInfo.getColumnByName("code_id") != null ?userInfo.getColumnByName("code_id").getLongValue() : 0L;
    			taxArray.put(value);
    		}
    	}
    		if(subjectCode != 0L && subjectCode != null)
    		eventMap.put("subject", subjectCode);
    		if(courseCode != 0L && courseCode != null)
    		eventMap.put("course", courseCode);
    		if(unitCode != 0L && unitCode != null)
    		eventMap.put("unit", unitCode);
    		if(topicCode != 0L && topicCode != null)
    		eventMap.put("topic", topicCode);
    		if(lessonCode != 0L && lessonCode != null)
    		eventMap.put("lesson", lessonCode);
    		if(conceptCode != 0L && conceptCode != null)
    		eventMap.put("concept", conceptCode);
    		if(taxArray != null && taxArray.toString() != null)
    		eventMap.put("standards", taxArray.toString());
    	
    	return eventMap;
    }
    
    public void postMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings",0);
    	ColumnList<String> jobIds = baseDao.readWithKey(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), "job_ids",0);
    	
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
    		
    		/*baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "start_count", ""+startVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "end_count", ""+endVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Inprogress");*/
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "total_job_count", ""+totalJobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "running_job_count", ""+jobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "indexed_count", ""+endVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), "job_ids", "job_names", runningJobs+","+jobId);
    		
    		Rows<String, String> resource = null;
    		MutationBatch m = null;
    		try {
    		m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);

    		for(long i = startVal ; i < endVal ; i++){
    			logger.info("contentId : "+ i);
    				resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
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
    			
    		/*	baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Completed");
    			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "run_time", (stop-start)+" ms");*/
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "total_time", ""+(totalTime + (stop-start)));
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "running_job_count", ""+(jobCount - 1));
    			
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}else{    		
    		logger.info("Job queue is full! Or Job Reached its allowed end");
    	}
		
    }
    
    public void catalogMigration(String startTime , String endTime,String cfName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings",0);
    	
    	cfName = settings.getColumnByName("cf_name").getStringValue();
    		
    	Map<String,String> columnType = tablesDataTypeCache.get(cfName);
    	
    	long jobCount = Long.valueOf(settings.getColumnByName("running_job_count").getStringValue());
    	long totalJobCount = Long.valueOf(settings.getColumnByName("total_job_count").getStringValue());
    	long maxJobCount = Long.valueOf(settings.getColumnByName("max_job_count").getStringValue());
    	long allowedCount = Long.valueOf(settings.getColumnByName("allowed_count").getStringValue());
    	long indexedCount = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    	long totalTime = Long.valueOf(settings.getColumnByName("total_time").getStringValue());
    		
    	if((jobCount < maxJobCount) && (indexedCount < allowedCount) ){
    		long start = System.currentTimeMillis();
    		long endIndex = Long.valueOf(settings.getColumnByName("max_count").getStringValue());
    		long startVal = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    		long endVal = (endIndex + startVal);
    		jobCount = (jobCount + 1);
    		totalJobCount = (totalJobCount + 1);
    		
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "total_job_count", ""+totalJobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "running_job_count", ""+jobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "indexed_count", ""+endVal);
    		
    		Rows<String, String> resource = null;
    		MutationBatch m = null;
    		try {

    		for(long i = startVal ; i < endVal ; i++){
    			m = getConnectionProvider().getAwsKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
    			logger.info("collection item Id : "+ i);
    				resource = baseDao.readIndexedColumn(cfName, "collection_content_id", i,0);
    				if(resource != null && resource.size() > 0){
    					for(int k = 0 ; k < resource.size() ; k++){
    						logger.info("\n Key : "+ resource.getRowByIndex(k).getKey());
    						for(int l =0 ; l < resource.getRowByIndex(k).getColumns().size() ;l++){
    							if(columnType.get(resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getName().trim()).equalsIgnoreCase("text")){    								
    								baseDao.generateNonCounter(cfName, resource.getRowByIndex(k).getKey(), resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getName(), resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getStringValue(), m);
    							}
    							if(columnType.get(resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getName().trim()).equalsIgnoreCase("bigint")){    								
    								baseDao.generateNonCounter(cfName, resource.getRowByIndex(k).getKey(), resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getName(), resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getLongValue(), m);
    							}
    							if(columnType.get(resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getName().trim()).equalsIgnoreCase("int")){    								
    								baseDao.generateNonCounter(cfName, resource.getRowByIndex(k).getKey(), resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getName(), resource.getRowByIndex(k).getColumns().getColumnByIndex(l).getIntegerValue(), m);
    							}
    						}
    						
    					}
    				}    			
    				m.execute();
    		}
    			long stop = System.currentTimeMillis();
    			
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "total_time", ""+(totalTime + (stop-start)));
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ci_job_settings", "running_job_count", ""+(jobCount - 1));
    			
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}else{    		
    		logger.info("Job queue is full! Or Job Reached its allowed end");
    	}
		
    }
    public void postStatMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat_job_settings",0);
    	
    	Collection<String> columnList = new ArrayList<String>();
    	
    	long jobCount = Long.valueOf(settings.getColumnByName("running_job_count").getStringValue());
    	long totalJobCount = Long.valueOf(settings.getColumnByName("total_job_count").getStringValue());
    	long maxJobCount = Long.valueOf(settings.getColumnByName("max_job_count").getStringValue());
    	long allowedCount = Long.valueOf(settings.getColumnByName("allowed_count").getStringValue());
    	long indexedCount = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    		
    	if((jobCount < maxJobCount) && (indexedCount < allowedCount) ){
    		long endIndex = Long.valueOf(settings.getColumnByName("max_count").getStringValue());
    		long startVal = Long.valueOf(settings.getColumnByName("indexed_count").getStringValue());
    		long endVal = (endIndex + startVal);
    		jobCount = (jobCount + 1);
    		totalJobCount = (totalJobCount + 1);
    		
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "total_job_count", ""+totalJobCount);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "running_job_count", ""+jobCount);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "indexed_count", ""+endVal);
    		
    		try {
	    		for(long i = startVal ; i <= endVal ; i++){
	    			logger.info("contentId : "+ i);
	    			String gooruOid = null;
	    			Rows<String, String> resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
	    			if(resource != null && resource.size() > 0){
    					ColumnList<String> columns = resource.getRowByIndex(0).getColumns();
    					
    					gooruOid = columns.getColumnByName("gooru_oid").getStringValue(); 
	    				
    					logger.info("gooruOid : {}",gooruOid);
	    				
	    				if(gooruOid != null){
	    					long insightsView = 0L;
	    					long gooruView = columns.getLongValue("views_count", 0L);
	    					
	    					ColumnList<String> vluesList = baseDao.readWithKeyColumnList(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+gooruOid, columnList,0);

	    							insightsView = vluesList.getLongValue("count~views", 0L);
	    							if(insightsView < gooruView){
		    							long balancedView = (gooruView - insightsView);
		    							if(balancedView != 0){
		    								baseDao.increamentCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+gooruOid, "count~views", balancedView);
		    							}
	    						}
	    				}
    				
	    			}
	    		}
	    		
	    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat_job_settings", "running_job_count", ""+(jobCount - 1));
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
		MutationBatch m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
		ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "bal_stat_job_settings",0);
		for (Long startDate = Long.parseLong(settings.getStringValue("last_updated_time", null)) ; startDate <= Long.parseLong(minuteDateFormatter.format(new Date()));) {
			JSONArray resourceList = new JSONArray();
			logger.info("Start Date : {} ",String.valueOf(startDate));
			ColumnList<String> recentReources =  baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+String.valueOf(startDate),0);
			Collection<String> gooruOids =  recentReources.getColumnNames();
			
			for(String id : gooruOids){
				ColumnList<String> insightsData = baseDao.readWithKey(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+id,0);
				ColumnList<String> gooruData = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+id,0);
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
					ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+id,0);
	    			if(resource.getColumnByName("type_name") != null){
							String resourceType = resource.getColumnByName("type_name").getStringValue().equalsIgnoreCase("scollection") ? "scollection" : "resource";
							resourceObj.put("resourceType", resourceType);
					}
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
	
    public void MigrateSearchCF(Long start,Long end){
    	logger.info("Migration started...................");
		for(long i = start ; i < end ; i++){
			try {
			logger.info("contentId : "+ i);
			Rows<String, String> resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
				if(resource != null && resource.size() > 0){
					for(int a = 0 ; a < resource.size(); a++){
						String Key = resource.getRowByIndex(a).getKey();
						ColumnList<String> columns = resource.getRow(Key).getColumns();
						long viewCount = columns.getLongValue("views_count", 0L);
						long resourceType = columns.getLongValue("resource_type", 0L);
						String gooruOid = columns.getStringValue("gooru_oid", null);
						ColumnList<String> searchResource =  baseDao.readSearchKey("resource", gooruOid, 0);
						MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
						if(searchResource != null && searchResource.size() > 0){
							logger.info("Migrating resource : "+ gooruOid);
							for(int x = 0 ; x < searchResource.size(); x++){
								String columnName = searchResource.getColumnByIndex(x).getName();
								if(columnName.equalsIgnoreCase("stas.viewCount") || columnName.equalsIgnoreCase("stas.viewsCount") || columnName.equalsIgnoreCase("statistics.viewsCount")){
									logger.info("Do Nothing:"+columnName);
								}else if(columnName.equalsIgnoreCase("addDate") || columnName.equalsIgnoreCase("createdOn") || columnName.equalsIgnoreCase("lastModified")){
									baseDao.generateNonCounter("resource",gooruOid,columnName, searchResource.getColumnByIndex(x).getDateValue(), m);
								}else if(columnName.equalsIgnoreCase("contentId") || columnName.equalsIgnoreCase("statistics.copiedCount") || columnName.equalsIgnoreCase("statistics.copiedLevelCount")){
									baseDao.generateNonCounter("resource",gooruOid,columnName, searchResource.getColumnByIndex(x).getLongValue(), m);
								}else if(columnName.equalsIgnoreCase("collaboratorCount") || columnName.equalsIgnoreCase("creator.userId") || columnName.equalsIgnoreCase("frameBreaker") 
										|| columnName.equalsIgnoreCase("isCanonical")|| columnName.equalsIgnoreCase("numOfPages") || columnName.equalsIgnoreCase("owner.userId") 
										|| columnName.equalsIgnoreCase("resourceSourceId") || columnName.equalsIgnoreCase("statistics.hasFrameBreakerN") || columnName.equalsIgnoreCase("statistics.hasNoThumbnailN") 
										|| columnName.equalsIgnoreCase("statistics.statusIsBroken") || columnName.equalsIgnoreCase("statistics.usedInCollectionCountN") 
										|| columnName.equalsIgnoreCase("statistics.usedInDistCollectionCountN") || columnName.equalsIgnoreCase("statistics.usedInSCollectionCountN") ){
			
									baseDao.generateNonCounter("resource",gooruOid,columnName, searchResource.getColumnByIndex(x).getIntegerValue(), m);
								}
								else if(columnName.equalsIgnoreCase("isOer")){
									baseDao.generateNonCounter("resource",gooruOid,columnName, searchResource.getColumnByIndex(x).getStringValue(), m);
									baseDao.generateNonCounter("resource",gooruOid,columnName+"Boolean", searchResource.getColumnByIndex(x).getStringValue().equalsIgnoreCase("0") ? false:true, m);
								}else if(columnName.equalsIgnoreCase("ratings.average") || columnName.equalsIgnoreCase("ratings.count") || columnName.equalsIgnoreCase("ratings.reviewCount")){
									ByteBuffer value = searchResource.getByteBufferValue(columnName, null);
									baseDao.generateNonCounter("resource",gooruOid,columnName, value , m);									
								}else if(columnName.equalsIgnoreCase("statistics.subscriberCount") || columnName.equalsIgnoreCase("statistics.voteDown") || columnName.equalsIgnoreCase("statistics.voteUp")){
									baseDao.generateNonCounter("resource",gooruOid,columnName+"N", Long.valueOf(searchResource.getColumnByIndex(x).getStringValue()), m);
									baseDao.generateNonCounter("resource",gooruOid,columnName, searchResource.getColumnByIndex(x).getStringValue(), m);
								}else{
									baseDao.generateNonCounter("resource",gooruOid,columnName, searchResource.getColumnByIndex(x).getStringValue(), m);
								}
								baseDao.generateNonCounter("resource",gooruOid,"version", 1, m);
								baseDao.generateNonCounter("resource",gooruOid,"isDeleted", 0, m);
								baseDao.generateNonCounter("resource",gooruOid,"gooruOId", gooruOid , m);
								baseDao.generateNonCounter("resource",gooruOid,"resourceTypeN", resourceType , m);
								baseDao.generateNonCounter("resource",gooruOid,"statistics.viewsCountN", viewCount , m);
								baseDao.generateNonCounter("resource",gooruOid,"statistics.viewsCount", ""+viewCount , m);
							}
									
						}else{/*
							SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");
							SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
							SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.000");
							
							baseDao.generateNonCounter("resource",gooruOid,"version", 1, m);
							baseDao.generateNonCounter("resource",gooruOid,"isDeleted", 1, m);
							baseDao.generateNonCounter("resource",gooruOid,"gooruOId", gooruOid , m);
							baseDao.generateNonCounter("resource",gooruOid,"resourceTypeN", resourceType , m);
							baseDao.generateNonCounter("resource",gooruOid,"statistics.viewsCountN", viewCount , m);
							baseDao.generateNonCounter("resource",gooruOid,"statistics.viewsCount", ""+viewCount , m);
							
							if(columns.getColumnByName("title") != null){
								baseDao.generateNonCounter("resource",gooruOid,"title", columns.getColumnByName("title").getStringValue() , m);
							}
							if(columns.getColumnByName("description") != null){
								baseDao.generateNonCounter("resource",gooruOid,"description", columns.getColumnByName("description").getStringValue() , m);
							}
							
							if(columns.getColumnByName("last_modified") != null){
							try{
								baseDao.generateNonCounter("resource",gooruOid,"lastModified", formatter.parse(columns.getColumnByName("last_modified").getStringValue()) , m);
							}catch(Exception e){
								try{
									baseDao.generateNonCounter("resource",gooruOid,"lastModified", formatter2.parse(columns.getColumnByName("last_modified").getStringValue()) , m);
								}catch(Exception e2){
									baseDao.generateNonCounter("resource",gooruOid,"lastModified", formatter3.parse(columns.getColumnByName("last_modified").getStringValue()) , m);
								}
							}
							}
							if(columns.getColumnByName("created_on") != null){
							try{
								baseDao.generateNonCounter("resource",gooruOid,"createdOn",columns.getColumnByName("created_on") != null  ? formatter.parse(columns.getColumnByName("created_on").getStringValue()) : formatter.parse(columns.getColumnByName("last_modified").getStringValue()) , m);
							}catch(Exception e){
									try{
										baseDao.generateNonCounter("resource",gooruOid,"createdOn",columns.getColumnByName("created_on") != null  ? formatter2.parse(columns.getColumnByName("created_on").getStringValue()) : formatter2.parse(columns.getColumnByName("last_modified").getStringValue()), m);
									}catch(Exception e2){
										baseDao.generateNonCounter("resource",gooruOid,"createdOn",columns.getColumnByName("created_on") != null  ? formatter3.parse(columns.getColumnByName("created_on").getStringValue()) : formatter3.parse(columns.getColumnByName("last_modified").getStringValue()), m);
									}
								}
							}
							if(columns.getColumnByName("creator_uid") != null){
								baseDao.generateNonCounter("resource",gooruOid,"creator.userUid",columns.getColumnByName("creator_uid").getStringValue(), m);
							}
							if(columns.getColumnByName("user_uid") != null){
								baseDao.generateNonCounter("resource",gooruOid,"owner.userUid",columns.getColumnByName("user_uid").getStringValue(), m);
							}
							if(columns.getColumnByName("record_source") != null){
								baseDao.generateNonCounter("resource",gooruOid,"recordSource",columns.getColumnByName("record_source").getStringValue(), m);
							}
							if(columns.getColumnByName("sharing") != null){
								baseDao.generateNonCounter("resource",gooruOid,"sharing",columns.getColumnByName("sharing").getStringValue(), m);
							}

							if(columns.getColumnByName("organization_uid") != null){
								baseDao.generateNonCounter("resource",gooruOid,"organization.partyUid",columns.getColumnByName("organization_uid").getStringValue(), m);
							}
							if(columns.getColumnByName("thumbnail") != null){
								baseDao.generateNonCounter("resource",gooruOid,"thumbnail",columns.getColumnByName("thumbnail").getStringValue(), m);
							}
							if(columns.getColumnByName("grade") != null){
								baseDao.generateNonCounter("resource",gooruOid,"grade",columns.getColumnByName("grade").getStringValue(), m);
							}
							if(columns.getColumnByName("license_name") != null){
								baseDao.generateNonCounter("resource",gooruOid,"license.name",columns.getColumnByName("license_name").getStringValue(), m);
							}

							if(columns.getColumnByName("category") != null){
								baseDao.generateNonCounter("resource",gooruOid,"category",columns.getColumnByName("category").getStringValue(), m);
							}
							if(columns.getColumnByName("type_name") != null){
								baseDao.generateNonCounter("resource",gooruOid,"resourceType",columns.getColumnByName("type_name").getStringValue(), m);
							}		
							baseDao.generateNonCounter("resource",gooruOid,"stas.viewCount", viewCount, m);
							baseDao.generateNonCounter("resource",gooruOid,"statistics.viewsCount", viewCount, m);
							
						*/	logger.info("Resource NOT FOUND in search: "+ gooruOid);	
						}
						m.execute();
					}
				}
			
			} catch(Exception e){
				e.printStackTrace();
			}
		
		}
    
    }


    public void MigrateResourceCF(long start,long end) {
    	
		for(long i = start ; i < end ; i++){
		
			try {
			logger.info("contentId : "+ i);
			Rows<String, String> resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
				if(resource != null && resource.size() > 0){
					for(int a = 0 ; a < resource.size(); a++){
						MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
						String Key = resource.getRowByIndex(a).getKey();
						ColumnListMutation<String> cm = m.withRow(baseDao.accessColumnFamily(ColumnFamily.DIMRESOURCE.getColumnFamily()), Key);
						ColumnList<String> columns = resource.getRow(Key).getColumns();
						for(int j = 0 ; j < columns.size() ; j++){
							
							Object value = null;
							
							String columnNameType = resourceCodeType.get(columns.getColumnByIndex(j).getName());

							if(columnNameType == null){
								logger.info("Do nothing..");
							}
							else if(columnNameType.equalsIgnoreCase("String")){
			            		value = columns.getColumnByIndex(j).getStringValue();
			            	}
							else if(columnNameType.equalsIgnoreCase("Long")){
			            		value = columns.getColumnByIndex(j).getLongValue();
			            	}
							else if(columnNameType.equalsIgnoreCase("Integer")){
			            		value = columns.getColumnByIndex(j).getIntegerValue();
			            	}
							else if(columnNameType.equalsIgnoreCase("Boolean")){
			            		value = columns.getColumnByIndex(j).getBooleanValue();
			            	}
			            	else{
			            		value = columns.getColumnByIndex(j).getStringValue();
			            	}
							if(value != null){
								baseDao.generateNonCounter(columns.getColumnByIndex(j).getName(),value,cm);
							}
			            
						}
						m.execute();		
					}
				}
			
			} catch(Exception e){
				logger.info("error while migrating content : " + e );
			}
		
		}
    }

	
	//Migrate live-dashboard
	public void migrateLiveDashBoard(){
		try{
			ColumnList<String> settings = baseDao.readWithKey("v1",ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "migrate_live_dashboard", 0);
			Long resourceCount = Long.valueOf(settings.getStringValue("max_count", null));
			Long indexedCount = Long.valueOf(settings.getStringValue("indexed_count", null));
			Long runningStatus = Long.valueOf(settings.getStringValue("running_status", null));
			logger.info("resourceCount : " + resourceCount + "indexedCount : " + indexedCount);
			if(runningStatus > 0){
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"migrate_live_dashboard" , "indexed_count", "" + (indexedCount + resourceCount));
				logger.info("Started migration from contentId: {}", indexedCount);
				migrateLiveDashBoard(indexedCount, (indexedCount + resourceCount));
			}
		}
		catch (Exception e) {
			logger.info("Error in migrating live dashboard : {}",e);
		}
	}
	
	
	
	@Async
	public void migrateLiveDashBoard(final Long startValue, final Long endValue){
		final Thread liveDashboardMigrationThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try{
					MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
					for(Long contentId = startValue; contentId <= endValue; contentId++){
						Rows<String, String> resources = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", contentId, 0);
						String gooruOid = null;
						if(resources.size() > 0){
							for(Row<String, String> resource : resources){
								gooruOid = resource.getColumns().getColumnByName("gooru_oid").getStringValue();
								ColumnList<String> counterV1Row = baseDao.readWithKey("v1",ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~" + gooruOid, 0);
								ColumnList<String> counterV2Row = baseDao.readWithKey("v2",ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~" + gooruOid, 0);
								for(int columnIndex = 0 ; columnIndex < counterV1Row.size() ; columnIndex++ ){
									long balancedData = ((counterV1Row.getColumnByIndex(columnIndex).getLongValue()) - (counterV2Row.getLongValue(counterV1Row.getColumnByIndex(columnIndex).getName(),0L)));
									baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~" + gooruOid, counterV1Row.getColumnByIndex(columnIndex).getName(), balancedData, m);
								}
								logger.info("Migrated resource: ===>>> {} ", gooruOid);
							}
							m.execute();
						}
					} 
				} 
				catch(Exception e){
					logger.info("Error in migrating live dashboard : {}",e);
				}
			}
		});  
		liveDashboardMigrationThread.setDaemon(true);
		liveDashboardMigrationThread.start();
	}
	
	
	//Migrate live-dashboard - timespent and views
	public void migrateViewsTimespendLiveDashBoard(){
		try{
			ColumnList<String> settings = baseDao.readWithKey("v2",ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "migrate_ts_views_live_dashboard", 0);
			Long resourceCount = Long.valueOf(settings.getStringValue("resource_count", null));
			Long maximumCount = Long.valueOf(settings.getStringValue("max_count", null));
			Long indexedCount = Long.valueOf(settings.getStringValue("indexed_count", null));
			if(indexedCount.equals(maximumCount)) {
				baseDao.saveLongValue("v2",ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "migrate_ts_views_live_dashboard", "running_status", 0);
			}
			Long runningStatus = Long.valueOf(settings.getStringValue("running_status", null));
			logger.info("resourceCountVT : " + resourceCount + "indexedCountVT : " + indexedCount + "maxCountVT : " + maximumCount);
			if(runningStatus > 0){
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"migrate_ts_views_live_dashboard" , "indexed_count", "" + (indexedCount + resourceCount));
				logger.info("Started migration from contentId: {}", indexedCount);
				migrateViewsTimespendLiveDashBoard(indexedCount, (indexedCount + resourceCount));
			}
		}
		catch (Exception e) {
			logger.info("Error in migrating live dashboard : {}",e);
		}
	}
	
	
	
	@Async
	public void migrateViewsTimespendLiveDashBoard(final Long startValue, final Long endValue){
		final Thread liveDashboardVTMigrationThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try{
					MutationBatch m = getConnectionProvider().getAwsKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
					for(Long contentId = startValue; contentId <= endValue; contentId++){
						Rows<String, String> resources = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", contentId, 0);
						String gooruOid = null;
						Long viewCount = 0L;
						Long timeSpent = 0L;
						Long avgTimeSpent = 0L;

						if(resources.size() > 0){
							for(Row<String, String> resource : resources){
								gooruOid = resource.getColumns().getColumnByName("gooru_oid").getStringValue();
								ColumnList<String> counterV1Row = baseDao.readWithKey("v2",ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~" + gooruOid, 0);
								timeSpent = counterV1Row.getColumnByName("time_spent~total").getLongValue();
								viewCount = counterV1Row.getColumnByName("count~views").getLongValue();
								if((viewCount > 0L) && (timeSpent > 0L)) {
									avgTimeSpent = timeSpent/viewCount;
									baseDao.saveLongValue("v2",ColumnFamily.RESOURCE.getColumnFamily(), gooruOid, "statistics.totalTimeSpent", timeSpent);
									baseDao.saveLongValue("v2",ColumnFamily.RESOURCE.getColumnFamily(), gooruOid, "statistics.viewsCountN", viewCount);
									baseDao.saveLongValue("v2",ColumnFamily.RESOURCE.getColumnFamily(), gooruOid, "statistics.averageTimeSpent", avgTimeSpent);
									baseDao.saveStringValue("v2",ColumnFamily.RESOURCE.getColumnFamily(), gooruOid, "statistics.viewsCount", viewCount.toString());
								}
								logger.info("Migrated resource VT: ===>>> {} ", gooruOid);
							}
							m.execute();
						}
					} 
				} 
				catch(Exception e){
					logger.info("Error in migrating live dashboard : {}",e);
				}
			}
		});  
		liveDashboardVTMigrationThread.setDaemon(true);
		liveDashboardVTMigrationThread.start();
	}
	
	//Creating staging Events
    public HashMap<String, Object> createStageEvents(String minuteId,String hourId,String dateId,String eventId ,String userUid,ColumnList<String> eventDetails ,String eventDetailUUID) {
    	HashMap<String, Object> stagingEvents = new HashMap<String, Object>();
    	stagingEvents.put("minuteId", minuteId);
    	stagingEvents.put("hourId", hourId);
    	stagingEvents.put("dateId", dateId);
    	stagingEvents.put("eventId", eventId);
    	stagingEvents.put("userUid", userUid);
    	stagingEvents.put("contentGooruOid",eventDetails.getStringValue("content_gooru_oid", null));
    	stagingEvents.put("parentGooruOid",eventDetails.getStringValue("parent_gooru_oid", null));
    	stagingEvents.put("timeSpentInMillis",eventDetails.getLongValue("time_spent_in_millis", 0L));
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

    public void callAPIViewCount() throws Exception {
    	if(cache.get("stat_job").equalsIgnoreCase("stop")){
    		logger.info("job stopped");
    		return;
    	}
    	JSONArray resourceList = new JSONArray();
    	String lastUpadatedTime = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "view~count~last~updated", DEFAULTCOLUMN,0).getStringValue();
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
		
		logger.info("1-processing mins : {} ,current mins :{} ",minuteDateFormatter.format(rowValues),minuteDateFormatter.format(currDate));

		if((rowValues.getTime() < currDate.getTime())){
			ColumnList<String> contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+minuteDateFormatter.format(rowValues),0);
			ColumnList<String> indexedCountList = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+"indexed~limit",0);
			int indexedCount = indexedCountList != null ? Integer.valueOf(indexedCountList.getStringValue(minuteDateFormatter.format(rowValues), "0")) : 0;
			
			boolean status = this.getRecordsToProcess(rowValues, resourceList,"indexed~limit");
			
			if((contents.size() == 0 || indexedCount == (contents.size() - 1)) && status){
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "view~count~last~updated", DEFAULTCOLUMN, minuteDateFormatter.format(rowValues));
			}
		}	
		
		logger.info("2-processing curr mins : {}",minuteDateFormatter.format(currDate));
		boolean status = this.getRecordsToProcess(currDate, resourceList,"curr~indexing~limit");
		
   }
  private boolean getRecordsToProcess(Date rowValues,JSONArray resourceList,String indexLabelLimit) throws Exception{
	  
		  	String indexCollectionType = null;
			String indexResourceType = null;
			String IndexingStatus = null;
			String resourceIds = "";
			String collectionIds = "";
			int indexedCount = 0;
			int indexedLimit = 2;
			int allowedLimit = 0;
		MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
		MutationBatch m2 = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
		ColumnList<String> contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+minuteDateFormatter.format(rowValues),0);
		ColumnList<String> indexedCountList = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+indexLabelLimit,0);
		indexedCount = indexedCountList != null ? Integer.valueOf(indexedCountList.getStringValue(minuteDateFormatter.format(rowValues), "0")) : 0;
		/*if(contents.size() == 0 || indexedCount == (contents.size() - 1)){
		 rowValues = new Date(rowValues.getTime() + 60000);
		 contents = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+minuteDateFormatter.format(rowValues),0);
		}*/
		logger.info("1:-> size : " + contents.size() + "indexed count : " + indexedCount);

		if(contents.size() > 0 ){
			ColumnList<String> IndexLimitList = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"index~limit",0);
			indexedLimit = IndexLimitList != null ? Integer.valueOf(IndexLimitList.getStringValue(DEFAULTCOLUMN, "0")) : 2;
			allowedLimit = (indexedCount + indexedLimit);
			if(allowedLimit > contents.size() ){
				allowedLimit = indexedCount + (contents.size() - indexedCount) ;
			}
			ColumnList<String> indexingStat = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"search~index~status",0);
			IndexingStatus = indexingStat.getStringValue(DEFAULTCOLUMN,null); 
			if(IndexingStatus.equalsIgnoreCase("completed")){
				for(int i = indexedCount ; i < allowedLimit ; i++) {
					indexedCount = i;
					ColumnList<String> vluesList = baseDao.readWithKeyColumnList(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+contents.getColumnByIndex(i).getStringValue(), statKeys,0);
					for(Column<String> detail : vluesList) {
						JSONObject resourceObj = new JSONObject();
						resourceObj.put("gooruOid", contents.getColumnByIndex(i).getStringValue());
						ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+contents.getColumnByIndex(i).getStringValue(),0);
		    			if(resource.getColumnByName("type_name") != null && resource.getColumnByName("type_name").getStringValue().equalsIgnoreCase("scollection")){
		    				indexCollectionType = "scollection";
		    				if(!collectionIds.contains(contents.getColumnByIndex(i).getStringValue())){
		    					collectionIds += ","+contents.getColumnByIndex(i).getStringValue();
		    				}
						}else{
							indexResourceType = "resource";
							if(!resourceIds.contains(contents.getColumnByIndex(i).getStringValue())){
								resourceIds += ","+contents.getColumnByIndex(i).getStringValue();
							}
						}
						for(String column : statKeys){
							if(detail.getName().equals(column)){
								if(statMetrics.getStringValue(column, null) != null){
									baseDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(),contents.getColumnByIndex(i).getStringValue(),statMetrics.getStringValue(column, null),detail.getLongValue(),m);
									if(statMetrics.getStringValue(column, null).equalsIgnoreCase("stas.viewsCount")){										
										baseDao.generateNonCounter(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+contents.getColumnByIndex(i).getStringValue(),"views_count",detail.getLongValue(),m2);
									}
								}
							}
						}
					
					}
				}
			}
		if(indexCollectionType != null || indexResourceType != null){
			m.execute();
			m2.execute();
			int indexingStatus = 0;
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "search~index~status", DEFAULTCOLUMN, "in-progress");
			if(indexCollectionType != null){
				indexingStatus  = this.callIndexingAPI(indexCollectionType, collectionIds.substring(1), rowValues);
			}
			if(indexResourceType != null){
				indexingStatus  = this.callIndexingAPI(indexResourceType, resourceIds.substring(1), rowValues);
			}
			baseDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS+SEPERATOR+indexLabelLimit, minuteDateFormatter.format(rowValues) ,String.valueOf(indexedCount++),86400);

			if(indexingStatus == 200 || indexingStatus == 404){
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "search~index~status", DEFAULTCOLUMN, "completed");
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "index~waiting~count", DEFAULTCOLUMN, "0");
				return true;
			}else{
				logger.info("Statistical data update failed");
				return false;
			}
		}else if(IndexingStatus != null && IndexingStatus.equalsIgnoreCase("in-progress")){
			ColumnList<String> indexWaitingLimit = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"index~waiting~limit",0);
			String limit = indexWaitingLimit != null ? indexWaitingLimit.getStringValue(DEFAULTCOLUMN, "0") : "0";
			
			ColumnList<String> indexWaitingCount = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"index~waiting~count",0);
			String count = indexWaitingCount != null ? indexWaitingCount.getStringValue(DEFAULTCOLUMN, "0") : "0";
			
			if(Integer.valueOf(count) > Integer.valueOf(limit)){
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "search~index~status", DEFAULTCOLUMN, "completed");
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "index~waiting~count", DEFAULTCOLUMN, "0");
			}else{
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "index~waiting~count", DEFAULTCOLUMN, ""+(Integer.valueOf(count)+1));
			}
	 		logger.info("Waiting for indexing"+ (Integer.valueOf(count)+1));
	 		return false;
		}
	}else{
		logger.info("No content is viewed");
		return true;
	}
	return false;
}
    
  /*
   * rowValues can be null
   */
    private int callIndexingAPI(String resourceType,String ids,Date rowValues){
    	
    	try{
    		String sessionToken = cache.get(SESSIONTOKEN);
    		String url = cache.get(SEARCHINDEXAPI) + resourceType + "/index?sessionToken=" + sessionToken + "&ids="+ids;
    		DefaultHttpClient httpClient = new DefaultHttpClient();
    		HttpPost  postRequest = new HttpPost(url);
    		logger.info("Indexing url : {} ",url);
    		HttpResponse response = httpClient.execute(postRequest);
	 		logger.info("Status : {} ",response.getStatusLine().getStatusCode());
	 		logger.info("Reason : {} ",response.getStatusLine().getReasonPhrase());
	 		if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 404) {
	 	 		logger.info("Search Indexing failed...");
	 	 		return response.getStatusLine().getStatusCode();
	 		} else {
	 	 		logger.info("Search Indexing call Success...");
	 	 		return response.getStatusLine().getStatusCode();
	 		}
    	}catch(Exception e){
    		logger.info("Search Indexing failed..." + e);
 	 		return 500;
    	}
    }
    private void callStatAPI(JSONArray resourceList,Date rowValues){
    	JSONObject staticsObj = new JSONObject();
		String sessionToken = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN,0).getStringValue();
		try{
				String url = cache.get(VIEWUPDATEENDPOINT) + "?skipReindex=false&sessionToken=" + sessionToken;
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
				 baseDao.saveBulkList(ColumnFamily.DIMEVENTS.getColumnFamily(),key,records);
		return true;
    	}
    	return false;
    }
    
	public boolean validateSchedular(String ipAddress) {

		try {
			/*
			ColumnList<String> columnList = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"schedular~ip",0);
			String configuredIp = columnList.getColumnByName("ip_address").getStringValue();*/
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
					 Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid,0);
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
			ColumnList<String> existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getEventId(),0);
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
        	ColumnList<String> existingParentRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getParentEventId(),0);
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
   	    	
   	    	ColumnList<String> activityRow = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventId,0);	
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
   					Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid,0);
   					for(Row<String, String> userDetail : userDetails){
   						userName = userDetail.getColumns().getStringValue("username", null);
   					}
   				 } catch (Exception e) {
   						logger.info("Error while fetching User uid ");
   				 }
   			 } else if (activityRow.getStringValue("gooru_uid", null) != null) {
   				try {
   					 userUid = activityRow.getStringValue("gooru_uid", null);
   					Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", activityRow.getStringValue("gooru_uid", null),0);
   					for(Row<String, String> userDetail : userDetails){
   						userName = userDetail.getColumns().getStringValue("username", null);
   					}
   				} catch (Exception e) {
   					logger.info("Error while fetching User uid ");
   				}			
   			 } else if (activityRow.getStringValue("user_id", null) != null) {
   				 try {
   					ColumnList<String> userUidList = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), activityRow.getStringValue("user_id", null),0);
					userUid = userUidList.getStringValue("gooru_uid", null);
					
   					Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", activityRow.getStringValue("gooru_uid", null),0);
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

 	public void balanceLiveBoardData(final String gooruOid){
 		
 	final Thread migrationThread = new Thread(new Runnable(){
        	@Override
        	public void run(){
	 		try {
				ColumnList<String> counterV1Row = baseDao.readWithKey("v1",ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+gooruOid, 0);
				ColumnList<String> counterV2Row = baseDao.readWithKey("v2",ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+gooruOid, 0);
				
				MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL);
	    		for(int i = 0 ; i < counterV1Row.size() ; i++ ){
	    			long balancedData = ((counterV1Row.getColumnByIndex(i).getLongValue()) - (counterV2Row.getLongValue(counterV1Row.getColumnByIndex(i).getName(),0L)));
	    			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+gooruOid, counterV1Row.getColumnByIndex(i).getName(), balancedData, m);
	    		}
	    		m.execute();
	    		
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        });
 		migrationThread.setDaemon(true);
 		migrationThread.start();
 	}
    @Async
    private String updateEvent(EventData eventData) {
    	ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(),eventData.getApiKey(),0);
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

  
