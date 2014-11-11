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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.utils.ExpiringMap;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
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

public class CassandraDataLoader  implements Constants {

    private static final Logger logger = LoggerFactory.getLogger(CassandraDataLoader.class);

    private static final Logger activityLogger = LoggerFactory.getLogger("activityLog");
	
    private static final Logger activityErrorLog = LoggerFactory.getLogger("activityErrorLog");
	
    private Keyspace cassandraKeyspace;
    
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
    
    private SimpleDateFormat minuteDateFormatter;
    
    private SimpleDateFormat dateFormatter;
    
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    
    private CassandraConnectionProvider connectionProvider;
    
    private KafkaLogProducer kafkaLogWriter;
  
    private MicroAggregatorDAOmpl liveAggregator;
    
    private LiveDashBoardDAOImpl liveDashBoardDAOImpl;
    
    public static  Map<String,String> cache;
    
    public static  Map<String,Object> licenseCache;
    
    public static  Map<String,Object> resourceTypesCache;
    
    public static  Map<String,Object> categoryCache;
    
    private static Map<String,Map<String,String>> kafkaConfigurationCache;
    
    public static  Map<String,Object> gooruTaxonomy;
    
    private MicroAggregatorProducer microAggregator;
    
    private static GeoLocation geo;
    
    public Collection<String> pushingEvents ;
    
    public Collection<String> statKeys ;
    
    public ColumnList<String> statMetrics ;
    
    private BaseCassandraRepoImpl baseDao ;
    
    public static  Map<String,String> taxonomyCodeType;
    
    public static  Map<String,String> resourceCodeType;
  
    private ExpiringMap<String, Object> localClassCache;
    
    private long DEFAULTEXPIRETIME = 1500000;
    
    /**
     * Get Kafka properties from Environment
     */
    public CassandraDataLoader() {
        this(null);
        
        //micro Aggregator producer IP
        //micro Aggregator producer IP
        String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty("kafka~microaggregator~producer").get("kafka_ip");
        String KAFKA_AGGREGATOR_PORT = getKafkaProperty("kafka~microaggregator~producer").get("kafka_portno");
        String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty("kafka~microaggregator~producer").get("kafka_topic");
        String KAFKA_AGGREGATOR_TYPE = getKafkaProperty("kafka~microaggregator~producer").get("kafka_producertype");

        //Log Writter producer IP
        String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty("kafka~logwritter~producer").get("kafka_ip");
        String KAFKA_LOG_WRITTER_PORT = getKafkaProperty("kafka~logwritter~producer").get("kafka_portno");
        String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty("kafka~logwritter~producer").get("kafka_topic");
        String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty("kafka~logwritter~producer").get("kafka_producertype");
        
        kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT,  KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
        microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT,  KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
    }

    public CassandraDataLoader(Map<String, String> configOptionsMap) {
        init(configOptionsMap);
        //micro Aggregator producer IP
        String KAFKA_AGGREGATOR_PRODUCER_IP = getKafkaProperty("kafka~microaggregator~producer").get("kafka_ip");
        String KAFKA_AGGREGATOR_PORT = getKafkaProperty("kafka~microaggregator~producer").get("kafka_portno");
        String KAFKA_AGGREGATOR_TOPIC = getKafkaProperty("kafka~microaggregator~producer").get("kafka_topic");
        String KAFKA_AGGREGATOR_TYPE = getKafkaProperty("kafka~microaggregator~producer").get("kafka_producertype");

        //Log Writter producer IP
        String KAFKA_LOG_WRITTER_PRODUCER_IP = getKafkaProperty("kafka~logwritter~producer").get("kafka_ip");
        String KAFKA_LOG_WRITTER_PORT = getKafkaProperty("kafka~logwritter~producer").get("kafka_portno");
        String KAFKA_LOG_WRITTER_TOPIC = getKafkaProperty("kafka~logwritter~producer").get("kafka_topic");
        String KAFKA_LOG_WRITTER_TYPE = getKafkaProperty("kafka~logwritter~producer").get("kafka_producertype");
        
        microAggregator = new MicroAggregatorProducer(KAFKA_AGGREGATOR_PRODUCER_IP, KAFKA_AGGREGATOR_PORT,  KAFKA_AGGREGATOR_TOPIC, KAFKA_AGGREGATOR_TYPE);
        kafkaLogWriter = new KafkaLogProducer(KAFKA_LOG_WRITTER_PRODUCER_IP, KAFKA_LOG_WRITTER_PORT,  KAFKA_LOG_WRITTER_TOPIC, KAFKA_LOG_WRITTER_TYPE);
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
        cache.put(ATMOSPHERENDPOINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "atmosphere.end.point", DEFAULTCOLUMN,0).getStringValue());
        cache.put(SESSIONTOKEN, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULTCOLUMN,0).getStringValue());
        cache.put(SEARCHINDEXAPI, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULTCOLUMN,0).getStringValue());
        cache.put(INDEXINGVERSION, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXINGVERSION, DEFAULTCOLUMN,0).getStringValue());
        geo = new GeoLocation();
        
        
        ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "schdulers~status",0);
        for(int i = 0 ; i < schdulersStatus.size() ; i++) {
        	cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
        }
        taxonomyCodeType = new LinkedHashMap<String, String>();
        
        ColumnList<String> taxonomyCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), "taxonomy_code",0);
        for(int i = 0 ; i < taxonomyCodeTypeList.size() ; i++) {
        	taxonomyCodeType.put(taxonomyCodeTypeList.getColumnByIndex(i).getName(), taxonomyCodeTypeList.getColumnByIndex(i).getStringValue());
        }
        localClassCache = new ExpiringMap<String, Object>(1000);
        
        resourceCodeType = new LinkedHashMap<String, String>();
        
        ColumnList<String> resourceCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), ColumnFamily.DIMRESOURCE.getColumnFamily(),0);
        for(int i = 0 ; i < resourceCodeTypeList.size() ; i++) {
        	resourceCodeType.put(resourceCodeTypeList.getColumnByIndex(i).getName(), resourceCodeTypeList.getColumnByIndex(i).getStringValue());
        }
        pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "default~key",0).getColumnNames();
        statMetrics = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat~metrics",0);
        statKeys = statMetrics.getColumnNames();
        
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
      if(kafkaConfigurationCache == null){
        	
            kafkaConfigurationCache = new HashMap<String,Map<String,String>>();
            String[] kafkaMessager =new String[]{"kafka~consumer","kafka~logwritter~producer","kafka~logwritter~consumer","kafka~microaggregator~producer","kafka~microaggregator~consumer"};
            Rows<String, String> result = baseDao.readCommaKeyList(CONFIG_SETTINGS, kafkaMessager);
            for(Row<String,String> row : result){
            	Map<String,String> properties = new HashMap<String, String>();
            	for(Column<String> column : row.getColumns()){
            		properties.put(column.getName(),column.getStringValue());
            	}
            	kafkaConfigurationCache.put(row.getKey(), properties);
            }
        System.out.println("kafa config"+kafkaConfigurationCache);
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
        cache.put(INDEXINGVERSION, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXINGVERSION, DEFAULTCOLUMN,0).getStringValue());
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
        
        ColumnList<String> taxonomyCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), "taxonomy_code",0);
        for(int i = 0 ; i < taxonomyCodeTypeList.size() ; i++) {
        	taxonomyCodeType.put(taxonomyCodeTypeList.getColumnByIndex(i).getName(), taxonomyCodeTypeList.getColumnByIndex(i).getStringValue());
        }
        
        ColumnList<String> resourceCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), ColumnFamily.DIMRESOURCE.getColumnFamily(),0);
        for(int i = 0 ; i < resourceCodeTypeList.size() ; i++) {
        	resourceCodeType.put(resourceCodeTypeList.getColumnByIndex(i).getName(), resourceCodeTypeList.getColumnByIndex(i).getStringValue());
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
    public void handleLogMessage(String fields, long startTime,
            String userAgent, String userIp, long endTime, String apiKey,
            String eventName, String gooruOId, String contentId, String query,String gooruUId,String userId,String gooruId,String type,
            String parentEventId,String context,String reactionType,String organizationUid,long timeSpentInMs,int[] answerId,int[] attemptStatus,int[] trySequence,String requestMethod, String eventId) {
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
	         long startTimeVal = 0;
	         long endTimeVal = 0;

	         if (eventData.getEventId() != null) {
	        	 existingRecord = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventData.getEventId(),0);
	        	 if (existingRecord != null && !existingRecord.isEmpty()) {
			         if ("start".equalsIgnoreCase(eventData.getEventType())) {
			        	 startTimeVal = existingRecord.getLongValue("start_time", null);
			         }
			         if ("stop".equalsIgnoreCase(eventData.getEventType())) {
			        	 endTimeVal = existingRecord.getLongValue("end_time", null);
			         }
			         if (startTimeVal == 0 && endTimeVal == 0) {
			         	// This is a duplicate event. Don't do anything!
			         	return;
			         }
			      }
	         }
	         Map<String,Object> records = new HashMap<String, Object>();
	         records.put("event_name", eventData.getEventName());
	         records.put("api_key",eventData.getApiKey() != null ? eventData.getApiKey() : DEFAULT_API_KEY );
	         Collection<String> existingEventRecord = baseDao.getKey(ColumnFamily.DIMEVENTS.getColumnFamily(),records,0);
	
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

    @Async
    public void handleEventObjectMessage(EventObject eventObject) throws JSONException, ConnectionException, IOException, GeoIp2Exception{

    	Map<String,String> eventMap = new LinkedHashMap<String, String>();
    	String aggregatorJson = null;
    	
    	try {

    		eventMap = JSONDeserializer.deserializeEventObject(eventObject);    	

	    	if (eventObject.getFields() != null) {
				logger.info("CORE: Writing to activity log - :"+ eventObject.getFields().toString());
				//kafkaLogWriter.sendEventLog(eventObject.getFields());
				activityLogger.info(eventObject.getFields());
				//Save Activity in ElasticSearch
				//this.saveActivityInIndex(eventObject.getFields());
				
			}
	    	
	    	eventMap = this.formatEventMap(eventObject, eventMap);
	    	
	    	String apiKey = eventObject.getApiKey() != null ? eventObject.getApiKey() : DEFAULT_API_KEY;
	    	
	    	if(aggregatorJson == null || aggregatorJson.isEmpty()){
	    	
	    		Map<String,Object> records = new HashMap<String, Object>();
		    	records.put("event_name", eventMap.get("eventName"));
		    	records.put("api_key",apiKey);
		    	Collection<String> eventId = baseDao.getKey(ColumnFamily.DIMEVENTS.getColumnFamily(),records,0);
		    	
		    	if(eventId == null || eventId.isEmpty()){
		    		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		    		records.put("event_id", uuid.toString());
		    		String key = apiKey +SEPERATOR+uuid.toString();
		    		baseDao.saveBulkList(ColumnFamily.DIMEVENTS.getColumnFamily(),key,records);
		    	}
			 
	    	}		
	    	updateEventObjectCompletion(eventObject);

	    	String eventKeyUUID = baseDao.saveEventObject(ColumnFamily.EVENTDETAIL.getColumnFamily(),null,eventObject);
			 
			if (eventKeyUUID == null) {
			    return;
			}
			
			Date eventDateTime = new Date(eventObject.getStartTime());
			String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
	
			if(eventObject.getEventType() == null || !eventObject.getEventType().equalsIgnoreCase("stop") || !eventObject.getEventType().equalsIgnoreCase("completed-event")){
			    baseDao.updateTimelineObject(ColumnFamily.EVENTTIMELINE.getColumnFamily(), eventRowKey,eventKeyUUID.toString(),eventObject);
			}
			
			aggregatorJson = cache.get(eventMap.get("eventName"));
			
			if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){		 	

				liveAggregator.realTimeMetrics(eventMap, aggregatorJson);	
			}
			
			
			if(aggregatorJson != null && !aggregatorJson.isEmpty() && aggregatorJson.equalsIgnoreCase(RAWUPDATE)){
				liveAggregator.updateRawData(eventMap);
			}
			
			liveDashBoardDAOImpl.callCountersV2(eventMap);
			
    	}catch(Exception e){
			logger.info("Writing error log : {} ",e);
			if (eventObject.getFields() != null) {
				activityErrorLog.info(eventObject.getFields());
				//kafkaLogWriter.sendErrorEventLog(eventObject.getFields());
			}
    	}

    	try {

			if(cache.get(VIEWEVENTS).contains(eventMap.get("eventName"))){
				liveDashBoardDAOImpl.addContentForPostViews(eventMap);
			}
			
			
			/*
			 * To be Re-enable 
			 * 
			liveDashBoardDAOImpl.findDifferenceInCount(eventMap);
	
			liveDashBoardDAOImpl.addApplicationSession(eventMap);
	
			liveDashBoardDAOImpl.saveGeoLocations(eventMap);
			

			if(pushingEvents.contains(eventMap.get("eventName"))){
				liveDashBoardDAOImpl.pushEventForAtmosphere(cache.get(ATMOSPHERENDPOINT),eventMap);
			}
	
			if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
				liveDashBoardDAOImpl.pushEventForAtmosphereProgress(atmosphereEndPoint, eventMap);
			}
			
			*/
	
    	}catch(Exception e){
    		logger.info("Exception in handleEventObjectHandler Post Process : {} ",e);
    	}
   }
    /**
     * 
     * @param eventData
     * 		Update the event is completion status 
     * @throws ConnectionException
     * 		If the host is unavailable
     */
    
    private void updateEventObjectCompletion(EventObject eventObject) throws ConnectionException {

    	long endTime = eventObject.getEndTime(), startTime = eventObject.getStartTime();
        long timeInMillisecs = 0L;
        if (endTime != 0 && startTime != 0) {
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
			if (endTime != 0 && startTime != 0) {
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
        		long parentStartTime = existingParentRecord.getLongValue("start_time", null);
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
    	long weekId = 0;
    	long monthId = 0;
    	String eventId = null;
    	long yearId = 0;
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

    
    public void updateStagingES(String startTime , String endTime,String customEventName,boolean isSchduler) throws ParseException {
    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

    	if(isSchduler){
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~indexing~status", DEFAULTCOLUMN,"in-progress");
    	}
    	
    	for (long startDate = dateFormatter.parse(startTime).getTime() ; startDate < dateFormatter.parse(endTime).getTime();) {

    		String currentDate = dateFormatter.format(new Date(startDate));
    		logger.info("Processing Date : {}" , currentDate);
    		
   		 	String timeLineKey = null;   		 	
   		 	if(customEventName == null || customEventName  == "") {
   		 		timeLineKey = currentDate.toString();
   		 	} else {
   		 		timeLineKey = currentDate.toString()+"~"+customEventName;
   		 	}
   		 	
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey,0);
   		 	
	    	if(eventUUID != null &&  !eventUUID.isEmpty() ) {
	    		readEventAndIndex(eventUUID);
	    	}
	    	//Incrementing time - one minute
	    	startDate = new Date(startDate).getTime() + 60000;
	    	
	    	if(isSchduler){
	    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~indexing~last~updated", DEFAULTCOLUMN,""+dateFormatter.format(new Date(startDate)));
	    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~indexing~checked~count", "constant_value", ""+0);
	    	}
	    }
	    
    	if(isSchduler){
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~indexing~status", DEFAULTCOLUMN,"completed");
    	}
    	
    	logger.info("Indexing completed..........");
    }

    public void  readEventAndIndex(final ColumnList<String> eventUUID){
    	final Thread counterThread = new Thread(new Runnable() {
    	  	@Override
    	  	public void run(){
    	  		
    	  		for(int i = 0 ; i < eventUUID.size() ; i++) {
    	    		logger.info("eventDetailUUID  : " + eventUUID.getColumnByIndex(i).getStringValue());
    	    		ColumnList<String> event =  baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventUUID.getColumnByIndex(i).getStringValue(),0);
    	    		saveActivityInIndex(event.getStringValue("fields", null));
    	    	}
    	  	}
    	});

    	counterThread.setDaemon(true);
    	counterThread.start();
    }
    
    public void viewMigFromEvents(String startTime , String endTime,String customEventName) throws Exception {
    	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    	SimpleDateFormat dateIdFormatter = new SimpleDateFormat("yyyy-MM-dd 00:00:00+0000");
    	Calendar cal = Calendar.getInstance();
    	String resourceType = "resource";
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
   		 	if(customEventName.equalsIgnoreCase(LoaderConstants.CPV1.getName())){
   		 	resourceType = "scollection";
   		 	}
   		 	//Read Event Time Line for event keys and create as a Collection
   		 	ColumnList<String> eventUUID = baseDao.readWithKey(ColumnFamily.EVENTTIMELINE.getColumnFamily(), timeLineKey,0);
   		 	String ids = "";
	    	if(eventUUID != null &&  !eventUUID.isEmpty() ) {

		    	Collection<String> eventDetailkeys = new ArrayList<String>();
		    	for(int i = 0 ; i < eventUUID.size() ; i++) {
		    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
		    		logger.info("eventDetailUUID  : " + eventDetailUUID);
		    		eventDetailkeys.add(eventDetailUUID);
		    	}
		    	
		    	//Read all records from Event Detail
		    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailkeys,0);
		    	for (Row<String, String> row : eventDetailsNew) {

		    		logger.info("content_gooru_oid : " + row.getColumns().getStringValue("content_gooru_oid", null));
		    		
		    		String id = row.getColumns().getStringValue("content_gooru_oid", null);
		    		
		    		if(id != null){
		    			ids += ","+id;
		    		}
					
		    	}
		    	this.migrateViews(ids.substring(1),resourceType);
	    	}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
	    }
	    
    }
    public void migrateViews(String resourceIds ,String resourceType) throws Exception{
    	
    	boolean proceed = false;
    	String ids = "";
    	
    	MutationBatch m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	MutationBatch m2 = getConnectionProvider().getAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

    	for(String id : resourceIds.split(",")){
    		
    	ids += ","+id;
		logger.info("type : {} ",resourceType);
		logger.info("id : {} ",id);
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
	
		baseDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(),id,"stas.viewsCount",(insightsView+balancedView),m2);
		proceed = true;
    		
    	}
    	if(proceed){
    		m2.execute();
    		m.execute();
    		this.callIndexingAPI(resourceType, ids.substring(1),null);
    	}
    }
    
    public void pathWayMigration(String startTime , String endTime,String customEventName) throws ParseException {
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
   		 	
	    	if(eventUUID != null &&  !eventUUID.isEmpty() ) {

		    	Collection<String> eventDetailkeys = new ArrayList<String>();
		    	for(int i = 0 ; i < eventUUID.size() ; i++) {
		    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
		    		logger.info("eventDetailUUID  : " + eventDetailUUID);
		    		eventDetailkeys.add(eventDetailUUID);
		    	}
		    	
		    	//Read all records from Event Detail
		    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailkeys,0);
		    	
		    	for (Row<String, String> row : eventDetailsNew) {
		    		String fields = row.getColumns().getStringValue("fields", null);
		    		
		    		try {

		    		JSONObject jsonField = new JSONObject(fields);
	    		
		    		if(jsonField.has("version")){
		    		
	    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
		    		
	    				Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObjects); 
	    				
						eventMap = this.formatEventMap(eventObjects, eventMap);
						
						String aggregatorJson = cache.get(eventMap.get("eventName"));
						
							if(aggregatorJson != null && !aggregatorJson.isEmpty() && !aggregatorJson.equalsIgnoreCase(RAWUPDATE)){
								logger.info("Fields : " + fields);
								logger.info("SessionId : " + eventMap.get(SESSION));

								liveAggregator.realTimeMetricsMigration(eventMap, aggregatorJson);
							}
	    				}
					} catch (Exception e) {
						logger.info("Exception : " + e);
					} 
		    		
		    		}
		    	
	    	}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
	    }
	    
    }
    
    public void migrateEventsToCounter(String startTime , String endTime,String customEventName) throws ParseException {
    	logger.info("counter job started");
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
   		 	
	    	if(eventUUID != null &&  !eventUUID.isEmpty() ) {

		    	Collection<String> eventDetailkeys = new ArrayList<String>();
		    	for(int i = 0 ; i < eventUUID.size() ; i++) {
		    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
		    		logger.info("eventDetailUUID  : " + eventDetailUUID);
		    		eventDetailkeys.add(eventDetailUUID);
		    	}
		    	
		    	//Read all records from Event Detail
		    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventDetailkeys,0);
		    	
		    	for (Row<String, String> row : eventDetailsNew) {
		    		logger.info("Fields : " + row.getColumns().getStringValue("fields", null));
		    		String fields = row.getColumns().getStringValue("fields", null);
		        	if(fields != null){
		    			try {
		    				JSONObject jsonField = new JSONObject(fields);
		    	    			if(jsonField.has("version")){
		    	    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
		    	    				
		    	    				Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObjects);    	
		    	    				eventMap.put("eventName", eventObjects.getEventName());
		    	    		    	eventMap.put("startTime",String.valueOf(eventObjects.getStartTime()));		    	    		    	
		    	    		    	eventMap.put("eventId", eventObjects.getEventId());
		    	    	    		
		    	    	    		liveDashBoardDAOImpl.callCountersV2Custom(eventMap);
		    	    			} 
		    	    			else{
		    	    				   Iterator<?> keys = jsonField.keys();
		    	    				   Map<String,String> eventMap = new HashMap<String, String>();
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
		    		    	    		
		    	    				   liveDashBoardDAOImpl.callCountersV2Custom(eventMap);
		    	    		     }
		    				} catch (Exception e) {
		    					logger.info("Error while Migration : {} ",e);
		    				}
		    				}
		    		
		        
		    	}
	    	}
	    	//Incrementing time - one minute
	    	cal.setTime(dateFormatter.parse(""+startDate));
	    	cal.add(Calendar.MINUTE, 1);
	    	Date incrementedTime =cal.getTime(); 
	    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
	    }
	    
    }
    @Async
    public void saveActivityInIndex(String fields){
    	if(fields != null){
			try {
				JSONObject jsonField = new JSONObject(fields);
	    			if(jsonField.has("version")){
	    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
	    				Map<String,Object> eventMap = JSONDeserializer.deserializeEventObjectv2(eventObjects);    	
	    				
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
    				   if(String.valueOf(eventMap.get(CONTENTGOORUOID)) != null){
    						ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), String.valueOf(eventMap.get(CONTENTGOORUOID)),0);
    				    	if(questionList != null && questionList.size() > 0){
    				    		eventMap.put("questionCount",questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
    				    		eventMap.put("resourceCount",questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
    				    		eventMap.put("oeCount",questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
    				    		eventMap.put("mcCount",questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);
   
    				    		eventMap.put("fibCount",questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
    				    		eventMap.put("maCount",questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
    				    		eventMap.put("tfCount",questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);
   
    				    		eventMap.put("itemCount",questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L );
    				    	}
    					}
	    	    		liveDashBoardDAOImpl.saveInESIndex(eventMap,ESIndexices.EVENTLOGGERINFO.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
	    			} 
	    			else{
	    				   Iterator<?> keys = jsonField.keys();
	    				   Map<String,Object> eventMap = new HashMap<String, Object>();
	    				   while( keys.hasNext() ){
	    			            String key = (String)keys.next();
	    			            
	    			            eventMap.put(key,String.valueOf(jsonField.get(key)));
	    			            
	    			            if(key.equalsIgnoreCase("contentGooruId") || key.equalsIgnoreCase("gooruOId") || key.equalsIgnoreCase("gooruOid")){
	    			            	eventMap.put("gooruOid", String.valueOf(jsonField.get(key)));
	    			            }
	
	    			            if(key.equalsIgnoreCase("eventName") && (String.valueOf(jsonField.get(key)).equalsIgnoreCase("create-reaction"))){
	    			            	eventMap.put("eventName", "reaction.create");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-play") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-play-dots")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collections-played")
	    			            						|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("quiz-play"))){
	    			            	
	    			            	eventMap.put("eventName", "collection.play");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("signIn-google-login") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("signIn-google-home")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("anonymous-login"))){
	    			            	eventMap.put("eventName", "user.login");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("signUp-home") 
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("signUp-login"))){
	    			            	eventMap.put("eventName", "user.register");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-play") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-player")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-play-dots")
	    			            						|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-question-resource-play-dots")
	    			            							|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-oe-play-dots")
	    			            								|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-question-play-dots"))){
	    			            	eventMap.put("eventName", "collection.resource.play");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("resource-player") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resource-play-dots")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resourceplayerstart")
	    			            						|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resourceplayerplay")
	    			            							|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resources-played")
	    			            								|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("question-oe-play-dots")
	    			            									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("question-play-dots"))){
	    			            	eventMap.put("eventName", "resource.play");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("gooruUId") || key.equalsIgnoreCase("gooruUid")){
	    			            	eventMap.put(GOORUID, String.valueOf(jsonField.get(key)));
	    			            }
	    			            
	    			        }
	    				   if(eventMap.get(CONTENTGOORUOID) != null){
	    				   		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    				   		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    				   }
	    				   if(eventMap.get(GOORUID) != null ){
	    					   eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
	    				   }	    	    	
	    				   
	    				   if(eventMap.get(EVENTNAME).equals(LoaderConstants.CPV1.getName()) && eventMap.get(CONTENTGOORUOID) != null){
	    						ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), String.valueOf(eventMap.get(CONTENTGOORUOID)),0);
	    				    	if(questionList != null && questionList.size() > 0){
	    				    		eventMap.put("questionCount",questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
	    				    		eventMap.put("resourceCount",questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
	    				    		eventMap.put("oeCount",questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
	    				    		eventMap.put("mcCount",questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);
	    				    		
	    				    		eventMap.put("fibCount",questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
	    				    		eventMap.put("maCount",questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
	    				    		eventMap.put("tfCount",questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);
	    				    		
	    				    		eventMap.put("itemCount",questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L );
	    				    	}
	    					}
		    	    		liveDashBoardDAOImpl.saveInESIndex(eventMap,ESIndexices.EVENTLOGGERINFO.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
	    		     }
				} catch (Exception e) {
					logger.info("Error while Migration : {} ",e);
				}
				}
		
    }
    
    public Map<String, Object> getUserInfo(Map<String,Object> eventMap , String gooruUId){
    	Collection<String> user = new ArrayList<String>();
    	user.add(gooruUId);
    	ColumnList<String> eventDetailsNew = baseDao.readWithKey(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), gooruUId,0);
    	//for (Row<String, String> row : eventDetailsNew) {
    		//ColumnList<String> userInfo = row.getColumns();
    	if(eventDetailsNew != null && eventDetailsNew.size() > 0){
    		for(int i = 0 ; i < eventDetailsNew.size() ; i++) {
    			String columnName = eventDetailsNew.getColumnByIndex(i).getName();
    			String value = eventDetailsNew.getColumnByIndex(i).getStringValue();
    			if(value != null){
    				eventMap.put(columnName, value);
    			}
    		}
    		}
    	//}
		return eventMap;
    }
    public Map<String,Object> getContentInfo(Map<String,Object> eventMap,String gooruOId){
    	
    	Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamily.COLLECTIONITEM.getColumnFamily(), gooruOId, 0);
    	
    	eventMap.put("contentItems",contentItems);
    	
    	ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+gooruOId,0);
    		if(resource != null){
    			eventMap.put("title", resource.getStringValue("title", null));
    			eventMap.put("description",resource.getStringValue("description", null));
    			eventMap.put("sharing", resource.getStringValue("sharing", null));
    			eventMap.put("category", resource.getStringValue("category", null));
    			eventMap.put("typeName", resource.getStringValue("type_name", null));
    			eventMap.put("license", resource.getStringValue("license_name", null));
    			eventMap.put("contentOrganizationId", resource.getStringValue("organization_uid", null));
    			
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
					long questionCounts = questionCount.getLongValue("questionCount", 0L);
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
    	Set<Long> subjectCode = new HashSet<Long>();
    	Set<Long> courseCode = new HashSet<Long>();
    	Set<Long> unitCode = new HashSet<Long>();
    	Set<Long> topicCode = new HashSet<Long>();
    	Set<Long> lessonCode = new HashSet<Long>();
    	Set<Long> conceptCode = new HashSet<Long>();
    	Set<Long> taxArray = new HashSet<Long>();

    	for (Row<String, String> row : eventDetailsNew) {
    		ColumnList<String> userInfo = row.getColumns();
    			long root = userInfo.getColumnByName("root_node_id") != null ? userInfo.getColumnByName("root_node_id").getLongValue() : 0L;
    			if(root == 20000L){
	    			long value = userInfo.getColumnByName("code_id") != null ?userInfo.getColumnByName("code_id").getLongValue() : 0L;
	    			long depth = userInfo.getColumnByName("depth") != null ?  userInfo.getColumnByName("depth").getLongValue() : 0L;
	    			if(value != 0L &&  depth == 1L){    				
	    				subjectCode.add(value);
	    			} 
	    			else if(depth == 2L){
	    			ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
	    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
	    			if(subject != 0L)
	    				subjectCode.add(subject);
	    			if(value != 0L)
	    				courseCode.add(value);
	    			}
	    			
	    			else if(depth == 3L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			if(subject != 0L)
		    			subjectCode.add(subject);
		    			if(course != 0L)
	    				courseCode.add(course);
		    			if(value != 0L)
	    				unitCode.add(value);
	    			}
	    			else if(depth == 4L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
		    				if(subject != 0L)
			    			subjectCode.add(subject);	
		    				if(course != 0L)
		    				courseCode.add(course);
		    				if(unit != 0L)
		    				unitCode.add(unit);
		    				if(value != 0L)
		    				topicCode.add(value);
	    			}
	    			else if(depth == 5L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
		    			long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : 0L;
		    				if(subject != 0L)
			    			subjectCode.add(subject);
			    			if(course != 0L)
		    				courseCode.add(course);
		    				if(unit != 0L)
		    				unitCode.add(unit);
		    				if(topic != 0L)
		    				topicCode.add(topic);
		    				if(value != 0L)
		    				lessonCode.add(value);
	    			}
	    			else if(depth == 6L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
		    			long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
		    			long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : 0L;
		    			long lesson = columns.getColumnByName("lesson_code_id") != null ? columns.getColumnByName("lesson_code_id").getLongValue() : 0L;
		    			if(subject != 0L)
		    			subjectCode.add(subject);
		    			if(course != 0L)
	    				courseCode.add(course);
	    				if(unit != 0L && unit != 0)
	    				unitCode.add(unit);
	    				if(topic != 0L)
	    				topicCode.add(topic);
	    				if(lesson != 0L)
	    				lessonCode.add(lesson);
	    				if(value != 0L)
	    				conceptCode.add(value);
	    			}
	    			else if(value != 0L){
	    				taxArray.add(value);
	    				
	    			}
    		}else{
    			long value = userInfo.getColumnByName("code_id") != null ?userInfo.getColumnByName("code_id").getLongValue() : 0L;
    			if(value != 0L){
    				taxArray.add(value);
    			}
    		}
    	}
    		if(subjectCode != null)
    		eventMap.put("subject", subjectCode);
    		if(courseCode != null)
    		eventMap.put("course", courseCode);
    		if(unitCode != null)
    		eventMap.put("unit", unitCode);
    		if(topicCode != null)
    		eventMap.put("topic", topicCode);
    		if(lessonCode != null)
    		eventMap.put("lesson", lessonCode);
    		if(conceptCode != null)
    		eventMap.put("concept", conceptCode);
    		if(taxArray != null)
    		eventMap.put("standards", taxArray);
    	
    	return eventMap;
    }
    
    public void postMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ts_job_settings",0);
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
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ts_job_settings", "total_job_count", ""+totalJobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ts_job_settings", "running_job_count", ""+jobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ts_job_settings", "indexed_count", ""+endVal);
    		baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), "job_ids", "job_names", runningJobs+","+jobId);
    		
    		Rows<String, String> resource = null;
    		MutationBatch m = null;
    		try {
    		m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

    		for(long i = startVal ; i < endVal ; i++){
    			logger.info("contentId : "+ i);
    				resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
    				if(resource != null && resource.size() > 0){
    					
    					ColumnList<String> columns = resource.getRowByIndex(0).getColumns();
    					
    					logger.info("Gooru Id: {} = Views : {} ",columns.getColumnByName("gooru_oid").getStringValue(),columns.getColumnByName("views_count").getLongValue());
    					
    					baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(), "count~views", columns.getColumnByName("views_count").getLongValue(), m);
    					baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(), "time_spent~total", (columns.getColumnByName("views_count").getLongValue() * 4000), m);
    					
    				}
    			
    		}
    			m.execute();
    			long stop = System.currentTimeMillis();
    			
    		/*	baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Completed");
    			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "run_time", (stop-start)+" ms");*/
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ts_job_settings", "total_time", ""+(totalTime + (stop-start)));
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "ts_job_settings", "running_job_count", ""+(jobCount - 1));
    			
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}else{    		
    		logger.info("Job queue is full! Or Job Reached its allowed end");
    	}
		
    }
    
    public void catalogMigration(String startTime , String endTime,String loaderType) {
    	
    	if(loaderType == null){
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "cat_job_settings",0);
    	
    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
    	
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
    		String jobId = "job-"+UUID.randomUUID();
    		
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "cat_job_settings", "total_job_count", ""+totalJobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "cat_job_settings", "running_job_count", ""+jobCount);
    		baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "cat_job_settings", "indexed_count", ""+endVal);
    		
    		Rows<String, String> resource = null;
    		MutationBatch m = null;
    		try {
    		m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

    		for(long i = startVal ; i < endVal ; i++){
    			logger.info("contentId : "+ i);
    				resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
    				if(resource != null && resource.size() > 0){
    					this.getResourceAndIndex(resource);
    				}
    			
    		}
    			m.execute();
    			long stop = System.currentTimeMillis();

    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "cat_job_settings", "total_time", ""+(totalTime + (stop-start)));
    			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "cat_job_settings", "running_job_count", ""+(jobCount - 1));
    			
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}else{    		
    		logger.info("Job queue is full! Or Job Reached its allowed end");
    	}
    	}else{
    		try{
    		long startVal = Long.valueOf(startTime);
    		long endVal = Long.valueOf(endTime);
    		Rows<String, String> resource = null;
    		MutationBatch m = null;
    		m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

    		for(long i = startVal ; i < endVal ; i++){
    			logger.info("contentId : "+ i);
    				resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
    				if(resource != null && resource.size() > 0){
    					this.getResourceAndIndex(resource);
    				}
    			
    		}
    			m.execute();
    		}catch(Exception e){
    			e.printStackTrace();
    		}
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
						String gooruOid = columns.getStringValue("gooru_oid", null);
						ColumnList<String> searchResource =  baseDao.readSearchKey("resource", gooruOid, 0);
						if(searchResource != null && searchResource.size() > 0){
							logger.info("Migrating resource : "+ gooruOid);
							MutationBatch m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
							for(int x = 0 ; x < searchResource.size(); x++){
								
								if(searchResource.getColumnByIndex(x).getName().equalsIgnoreCase("stas.viewCount")){
									baseDao.generateNonCounter("resource",gooruOid,searchResource.getColumnByIndex(x).getName(), viewCount, m);
								}else{
									baseDao.generateNonCounter("resource",gooruOid,searchResource.getColumnByIndex(x).getName(), searchResource.getColumnByIndex(x).getStringValue(), m);
								}
							}
							
							m.execute();		
						}else{
							logger.info("Resource NOT FOUND in search: "+ gooruOid);	
						}
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
								value = columns.getColumnByIndex(j).getStringValue();
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
							
			            	baseDao.generateNonCounter(columns.getColumnByIndex(j).getName(),value,cm);
			            
						}
						m.execute();		
					}
				}
			
			} catch(Exception e){
				logger.info("error while migrating content : " + e );
			}
		
		}
    }

    
    public void indexResource(String ids){
    	Collection<String> idList = new ArrayList<String>();
    	for(String id : ids.split(",")){
    		idList.add("GLP~" + id);
    	}
    	logger.info("resource id : {}",idList);
    	Rows<String,String> resource = baseDao.readWithKeyList(ColumnFamily.DIMRESOURCE.getColumnFamily(), idList,0);
    	try {
    		if(resource != null && resource.size() > 0){
    			this.getResourceAndIndex(resource);
    		}else {
    			throw new AccessDeniedException("Invalid Id!!");
    		}
		} catch (Exception e) {
			logger.info("indexing failed .. :{}",e);
		}
    }
    
    public void indexUser(String ids) throws Exception{
    	for(String userId : ids.split(",")){
    		getUserAndIndex(userId);
    	}
    }
    
    public void migrateRow(String sourceCluster,String targetCluster,String cfName,String key,String columnName,String type){
    	
    	MutationBatch m = null;
    	
    	ColumnList<String> sourceCoulmnList = baseDao.readWithKey(cfName, key,sourceCluster,0);
    	try{
	    	if(targetCluster.equalsIgnoreCase("DO")){
	    		m = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    	}else if(targetCluster.equalsIgnoreCase("AWSV1")){
	    		m = getConnectionProvider().getAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    	}else if(targetCluster.equalsIgnoreCase("AWSV2")){
	    		m = getConnectionProvider().getNewAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    	}
	    	
	    	if(columnName == null){
	    		for(int i = 0 ;i < sourceCoulmnList.size() ; i++){
	    			baseDao.generateNonCounter(cfName, key, sourceCoulmnList.getColumnByIndex(i).getName(), sourceCoulmnList.getColumnByIndex(i).getStringValue(), m);
	    		}
	    	}else if(columnName != null && type.equalsIgnoreCase("String")) {
	    		baseDao.generateNonCounter(cfName, key, columnName, sourceCoulmnList.getColumnByName(columnName).getStringValue(), m);
	    	}else if(columnName != null && type.equalsIgnoreCase("Long")) {
	    		baseDao.generateNonCounter(cfName, key, columnName, sourceCoulmnList.getColumnByName(columnName).getLongValue(), m);
	    	}
	    	
	    	m.execute();
	    	
    	}catch(Exception e){
    		e.printStackTrace();
    		logger.info("Error while migration " + e);
    	}
    }
    public void indexResourceView(String resourceIds,String type) throws Exception{
    	this.migrateViews(resourceIds,type);
    }
    
    
    private void getUserAndIndex(String userId) throws Exception{
    	logger.info("user id : "+ userId);
		ColumnList<String> userInfos = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), userId,0);
		
		if(userInfos != null & userInfos.size() > 0){
			
			XContentBuilder contentBuilder = jsonBuilder().startObject();
		
			if(userInfos.getColumnByName("gooru_uid") != null){
				logger.info( " Migrating User : " + userInfos.getColumnByName("gooru_uid").getStringValue()); 
				contentBuilder.field("user_uid",userInfos.getColumnByName("gooru_uid").getStringValue());
			}
			if(userInfos.getColumnByName("confirm_status") != null){
				contentBuilder.field("confirm_status",userInfos.getColumnByName("confirm_status").getLongValue());
			}
			if(userInfos.getColumnByName("registered_on") != null){
				contentBuilder.field("registered_on",TypeConverter.stringToAny(userInfos.getColumnByName("registered_on").getStringValue(), "Date"));
			}
			if(userInfos.getColumnByName("added_by_system") != null){
				contentBuilder.field("added_by_system",userInfos.getColumnByName("added_by_system").getLongValue());
			}
			if(userInfos.getColumnByName("account_created_type") != null){
				contentBuilder.field("account_created_type",userInfos.getColumnByName("account_created_type").getStringValue());
			}
			if(userInfos.getColumnByName("reference_uid") != null){
				contentBuilder.field("reference_uid",userInfos.getColumnByName("reference_uid").getStringValue());
			}
			if(userInfos.getColumnByName("email_sso") != null){
				contentBuilder.field("email_sso",userInfos.getColumnByName("email_sso").getStringValue());
			}
			if(userInfos.getColumnByName("deactivated_on") != null){
				contentBuilder.field("deactivated_on",TypeConverter.stringToAny(userInfos.getColumnByName("deactivated_on").getStringValue(), "Date"));
			}
			if(userInfos.getColumnByName("active") != null){
				contentBuilder.field("active",userInfos.getColumnByName("active").getIntegerValue());
			}
			if(userInfos.getColumnByName("last_login") != null){
				contentBuilder.field("last_login",TypeConverter.stringToAny(userInfos.getColumnByName("last_login").getStringValue(), "Date"));
			}
			if(userInfos.getColumnByName("identity_id") != null){
				contentBuilder.field("identity_id",userInfos.getColumnByName("identity_id").getIntegerValue());
			}
			if(userInfos.getColumnByName("mail_status") != null){
				contentBuilder.field("mail_status",userInfos.getColumnByName("mail_status").getLongValue());
			}
			if(userInfos.getColumnByName("idp_id") != null){
				contentBuilder.field("idp_id",userInfos.getColumnByName("idp_id").getIntegerValue());
			}
			if(userInfos.getColumnByName("state") != null){
				contentBuilder.field("state",userInfos.getColumnByName("state").getStringValue());
			}
			if(userInfos.getColumnByName("login_type") != null){
				contentBuilder.field("login_type",userInfos.getColumnByName("login_type").getStringValue());
			}
			if(userInfos.getColumnByName("user_group_uid") != null){
				contentBuilder.field("user_group_uid",userInfos.getColumnByName("user_group_uid").getStringValue());
			}
			if(userInfos.getColumnByName("primary_organization_uid") != null){
				contentBuilder.field("primary_organization_uid",userInfos.getColumnByName("primary_organization_uid").getStringValue());
			}
			if(userInfos.getColumnByName("license_version") != null){
				contentBuilder.field("license_version",userInfos.getColumnByName("license_version").getStringValue());
			}
			if(userInfos.getColumnByName("parent_id") != null){
				contentBuilder.field("parent_id",userInfos.getColumnByName("parent_id").getLongValue());
			}
			if(userInfos.getColumnByName("lastname") != null){
				contentBuilder.field("lastname",userInfos.getColumnByName("lastname").getStringValue());
			}
			if(userInfos.getColumnByName("account_type_id") != null){
				contentBuilder.field("account_type_id",userInfos.getColumnByName("account_type_id").getLongValue());
			}
			if(userInfos.getColumnByName("is_deleted") != null){
				contentBuilder.field("is_deleted",userInfos.getColumnByName("is_deleted").getIntegerValue());
			}
			if(userInfos.getColumnByName("external_id") != null){
				contentBuilder.field("external_id",userInfos.getColumnByName("external_id").getStringValue());
			}
			if(userInfos.getColumnByName("organization_uid") != null){
				contentBuilder.field("user_organization_uid",userInfos.getColumnByName("organization_uid").getStringValue());
			}
			if(userInfos.getColumnByName("import_code") != null){
				contentBuilder.field("import_code",userInfos.getColumnByName("import_code").getStringValue());
			}
			if(userInfos.getColumnByName("parent_uid") != null){
				contentBuilder.field("parent_uid",userInfos.getColumnByName("parent_uid").getStringValue());
			}
			if(userInfos.getColumnByName("security_group_uid") != null){
				contentBuilder.field("security_group_uid",userInfos.getColumnByName("security_group_uid").getStringValue());
			}
			if(userInfos.getColumnByName("username") != null){
				contentBuilder.field("username",userInfos.getColumnByName("username").getStringValue());
			}
			if(userInfos.getColumnByName("role_id") != null){
				contentBuilder.field("role_id",userInfos.getColumnByName("role_id").getLongValue());
			}
			if(userInfos.getColumnByName("firstname") != null){
				contentBuilder.field("firstname",userInfos.getColumnByName("firstname").getStringValue());
			}
			if(userInfos.getColumnByName("register_token") != null){
				contentBuilder.field("register_token",userInfos.getColumnByName("register_token").getStringValue());
			}
			if(userInfos.getColumnByName("view_flag") != null){
				contentBuilder.field("view_flag",userInfos.getColumnByName("view_flag").getLongValue());
			}
			if(userInfos.getColumnByName("account_uid") != null){
				contentBuilder.field("account_uid",userInfos.getColumnByName("account_uid").getStringValue());
			}

	    	Collection<String> user = new ArrayList<String>();
	    	user.add(userId);
	    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), user,0);
	    	for (Row<String, String> row : eventDetailsNew) {
	    		ColumnList<String> userInfo = row.getColumns();
	    		for(int i = 0 ; i < userInfo.size() ; i++) {
	    			String columnName = userInfo.getColumnByIndex(i).getName();
	    			String value = userInfo.getColumnByIndex(i).getStringValue();
	    			if(value != null){
	    				contentBuilder.field(columnName, value);
	    			}
	    		}
	    	}
		
	//		getConnectionProvider().getDevESClient().prepareIndex(ESIndexices.USERCATALOG.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.DIMUSER.getIndexType(), userId).setSource(contentBuilder).execute().actionGet()
			
			;
			getConnectionProvider().getProdESClient().prepareIndex(ESIndexices.USERCATALOG.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.DIMUSER.getIndexType(), userId).setSource(contentBuilder).execute().actionGet()
			
    		;
		}else {
			throw new AccessDeniedException("Invalid Id : " + userId);
		}	
			
		
		
	}
    
    public void indexTaxonomy(String sourceCf, String key, String targetIndex,String targetType) throws Exception{
    	
    	for(String id : key.split(",")){
    		ColumnList<String> sourceValues = baseDao.readWithKey(sourceCf, id,0);
	    	if(sourceValues != null && sourceValues.size() > 0){
	    		XContentBuilder contentBuilder = jsonBuilder().startObject();
	            for(int i = 0 ; i < sourceValues.size() ; i++) {
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("String")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getStringValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Long")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getLongValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Integer")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getIntegerValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Double")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getDoubleValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Date")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),TypeConverter.stringToAny(sourceValues.getColumnByIndex(i).getStringValue(), "Date"));
	            	}
	            }
	    		
	    		getConnectionProvider().getDevESClient().prepareIndex(targetIndex+"_"+cache.get(INDEXINGVERSION), targetType, id).setSource(contentBuilder).execute().actionGet()
				;
	    		getConnectionProvider().getProdESClient().prepareIndex(targetIndex+"_"+cache.get(INDEXINGVERSION), targetType, id).setSource(contentBuilder).execute().actionGet()
	    		;
	    	}
    	}
    }

    private void getResourceAndIndex(Rows<String, String> resource) throws ParseException{
    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
		SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.000");
		
		Map<String,Object> resourceMap = new LinkedHashMap<String, Object>();
		
		for(int a = 0 ; a < resource.size(); a++){
			
		ColumnList<String> columns = resource.getRowByIndex(a).getColumns();
		
		if(columns == null){
			return;
		}
		if(columns.getColumnByName("gooru_oid") != null){
			logger.info( " Migrating content : " + columns.getColumnByName("gooru_oid").getStringValue()); 
		}
		
		if(columns.getColumnByName("title") != null){
			resourceMap.put("title", columns.getColumnByName("title").getStringValue());
		}
		if(columns.getColumnByName("description") != null){
			resourceMap.put("description", columns.getColumnByName("description").getStringValue());
		}
		if(columns.getColumnByName("gooru_oid") != null){
			resourceMap.put("gooruOid", columns.getColumnByName("gooru_oid").getStringValue());
		}
		if(columns.getColumnByName("last_modified") != null){
		try{
			resourceMap.put("lastModified", formatter.parse(columns.getColumnByName("last_modified").getStringValue()));
		}catch(Exception e){
			try{
				resourceMap.put("lastModified", formatter2.parse(columns.getColumnByName("last_modified").getStringValue()));
			}catch(Exception e2){
				resourceMap.put("lastModified", formatter3.parse(columns.getColumnByName("last_modified").getStringValue()));
			}
		}
		}
		if(columns.getColumnByName("created_on") != null){
		try{
			resourceMap.put("createdOn", columns.getColumnByName("created_on") != null  ? formatter.parse(columns.getColumnByName("created_on").getStringValue()) : formatter.parse(columns.getColumnByName("last_modified").getStringValue()));
		}catch(Exception e){
			try{
				resourceMap.put("createdOn", columns.getColumnByName("created_on") != null  ? formatter2.parse(columns.getColumnByName("created_on").getStringValue()) : formatter2.parse(columns.getColumnByName("last_modified").getStringValue()));
			}catch(Exception e2){
				resourceMap.put("createdOn", columns.getColumnByName("created_on") != null  ? formatter3.parse(columns.getColumnByName("created_on").getStringValue()) : formatter3.parse(columns.getColumnByName("last_modified").getStringValue()));
			}
		}
		}
		if(columns.getColumnByName("creator_uid") != null){
			resourceMap.put("creatorUid", columns.getColumnByName("creator_uid").getStringValue());
		}
		if(columns.getColumnByName("user_uid") != null){
			resourceMap.put("userUid", columns.getColumnByName("user_uid").getStringValue());
		}
		if(columns.getColumnByName("record_source") != null){
			resourceMap.put("recordSource", columns.getColumnByName("record_source").getStringValue());
		}
		if(columns.getColumnByName("sharing") != null){
			resourceMap.put("sharing", columns.getColumnByName("sharing").getStringValue());
		}
		/*if(columns.getColumnByName("views_count") != null){
			resourceMap.put("viewsCount", columns.getColumnByName("views_count").getLongValue());
		}*/
		if(columns.getColumnByName("organization_uid") != null){
			resourceMap.put("contentOrganizationId", columns.getColumnByName("organization_uid").getStringValue());
		}
		if(columns.getColumnByName("thumbnail") != null){
			resourceMap.put("thumbnail", columns.getColumnByName("thumbnail").getStringValue());
		}
		if(columns.getColumnByName("grade") != null){
			JSONArray gradeArray = new JSONArray();
			for(String gradeId : columns.getColumnByName("grade").getStringValue().split(",")){
				gradeArray.put(gradeId);	
			}
			resourceMap.put("grade", gradeArray);
		}
		if(columns.getColumnByName("license_name") != null){
			//ColumnList<String> license = baseDao.readWithKey(ColumnFamily.LICENSE.getColumnFamily(), columns.getColumnByName("license_name").getStringValue());
			if(licenseCache.containsKey(columns.getColumnByName("license_name").getStringValue())){    							
				resourceMap.put("licenseId", licenseCache.get(columns.getColumnByName("license_name").getStringValue()));
			}
		}
		if(columns.getColumnByName("type_name") != null){
			//ColumnList<String> resourceType = baseDao.readWithKey(ColumnFamily.RESOURCETYPES.getColumnFamily(), columns.getColumnByName("type_name").getStringValue());
			if(resourceTypesCache.containsKey(columns.getColumnByName("type_name").getStringValue())){    							
				resourceMap.put("resourceTypeId", resourceTypesCache.get(columns.getColumnByName("type_name").getStringValue()));
			}
		}
		if(columns.getColumnByName("category") != null){
			//ColumnList<String> resourceType = baseDao.readWithKey(ColumnFamily.CATEGORY.getColumnFamily(), columns.getColumnByName("category").getStringValue());
			if(categoryCache.containsKey(columns.getColumnByName("category").getStringValue())){    							
				resourceMap.put("resourceCategoryId", categoryCache.get(columns.getColumnByName("category").getStringValue()));
			}
		}
		if(columns.getColumnByName("category") != null){
			resourceMap.put("category", columns.getColumnByName("category").getStringValue());
		}
		if(columns.getColumnByName("type_name") != null){
			resourceMap.put("typeName", columns.getColumnByName("type_name").getStringValue());
		}		
		if(columns.getColumnByName("gooru_oid") != null){
			ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), columns.getColumnByName("gooru_oid").getStringValue(),0);

	    	ColumnList<String> vluesList = baseDao.readWithKey(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(),0);
	    	
	    	if(vluesList != null && vluesList.size() > 0){
	    		
	    		long views = vluesList.getColumnByName("count~views") != null ?vluesList.getColumnByName("count~views").getLongValue() : 0L ;
	    		long totalTimespent = vluesList.getColumnByName("time_spent~total") != null ?vluesList.getColumnByName("time_spent~total").getLongValue() : 0L ;
	    		if(views > 0 && totalTimespent == 0L ){
	    			totalTimespent = (views * 180000);
	    			baseDao.increamentCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+columns.getColumnByName("gooru_oid").getStringValue(), "time_spent~total", totalTimespent);
	    		}
	    		resourceMap.put("viewsCount",views);
	    		resourceMap.put("totalTimespent",totalTimespent);
	    		resourceMap.put("avgTimespent",views != 0L ? (totalTimespent/views) : 0L );
	    		
	    		long ratings = vluesList.getColumnByName("count~ratings") != null ?vluesList.getColumnByName("count~ratings").getLongValue() : 0L ;
	    		long sumOfRatings = vluesList.getColumnByName("sum~rate") != null ?vluesList.getColumnByName("sum~rate").getLongValue() : 0L ;
	    		resourceMap.put("ratingsCount",ratings);
	    		resourceMap.put("sumOfRatings",sumOfRatings);
	    		resourceMap.put("avgRating",ratings != 0L ? (sumOfRatings/ratings) : 0L );
	    		
	    		
	    		long reactions = vluesList.getColumnByName("count~reactions") != null ?vluesList.getColumnByName("count~reactions").getLongValue() : 0L ;
	    		long sumOfreactionType = vluesList.getColumnByName("sum~reactionType") != null ?vluesList.getColumnByName("sum~reactionType").getLongValue() : 0L ;
	    		resourceMap.put("reactionsCount",ratings);
	    		resourceMap.put("sumOfreactionType",sumOfreactionType);
	    		resourceMap.put("avgReaction",reactions != 0L ? (sumOfreactionType/reactions) : 0L );
	    		
	    		
	    		resourceMap.put("countOfRating5",vluesList.getColumnByName("count~5") != null ?vluesList.getColumnByName("count~5").getLongValue() : 0L );
	    		resourceMap.put("countOfRating4",vluesList.getColumnByName("count~4") != null ?vluesList.getColumnByName("count~4").getLongValue() : 0L );
	    		resourceMap.put("countOfRating3",vluesList.getColumnByName("count~3") != null ?vluesList.getColumnByName("count~3").getLongValue() : 0L );
	    		resourceMap.put("countOfRating2",vluesList.getColumnByName("count~2") != null ?vluesList.getColumnByName("count~2").getLongValue() : 0L );
	    		resourceMap.put("countOfRating1",vluesList.getColumnByName("count~1") != null ?vluesList.getColumnByName("count~1").getLongValue() : 0L );
	    		
	    		resourceMap.put("countOfICanExplain",vluesList.getColumnByName("count~i-can-explain") != null ?vluesList.getColumnByName("count~i-can-explain").getLongValue() : 0L );
	    		resourceMap.put("countOfINeedHelp",vluesList.getColumnByName("count~i-need-help") != null ?vluesList.getColumnByName("count~i-need-help").getLongValue() : 0L );
	    		resourceMap.put("countOfIDoNotUnderstand",vluesList.getColumnByName("count~i-donot-understand") != null ?vluesList.getColumnByName("count~i-donot-understand").getLongValue() : 0L );
	    		resourceMap.put("countOfMeh",vluesList.getColumnByName("count~meh") != null ?vluesList.getColumnByName("count~meh").getLongValue() : 0L );
	    		resourceMap.put("countOfICanUnderstand",vluesList.getColumnByName("count~i-can-understand") != null ?vluesList.getColumnByName("count~i-can-understand").getLongValue() : 0L );
	    		resourceMap.put("copiedCount",vluesList.getColumnByName("copied~count") != null ?vluesList.getColumnByName("copied~count").getLongValue() : 0L );
	    		resourceMap.put("sharingCount",vluesList.getColumnByName("count~share") != null ?vluesList.getColumnByName("count~share").getLongValue() : 0L );
	    	}
	    	
	    	if(questionList != null && questionList.size() > 0){
	    		resourceMap.put("questionCount",questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
	    		resourceMap.put("resourceCount",questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
	    		resourceMap.put("oeCount",questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
	    		resourceMap.put("mcCount",questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);
	    		
	    		resourceMap.put("fibCount",questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
	    		resourceMap.put("maCount",questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
	    		resourceMap.put("tfCount",questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);
	    		
	    		resourceMap.put("itemCount",questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L );
	    	}
		}
		if(columns.getColumnByName("user_uid") != null){
			resourceMap = this.getUserInfo(resourceMap, columns.getColumnByName("user_uid").getStringValue());
		}
		if(columns.getColumnByName("gooru_oid") != null){
			resourceMap = this.getTaxonomyInfo(resourceMap, columns.getColumnByName("gooru_oid").getStringValue());
			liveDashBoardDAOImpl.saveInESIndex(resourceMap, ESIndexices.CONTENTCATALOGINFO.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.DIMRESOURCE.getIndexType(), columns.getColumnByName("gooru_oid").getStringValue());
		}
		}
    }
    
   
    public void postStatMigration(String startTime , String endTime,String customEventName) {
    	
    	ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "stat_job_settings",0);
    	ColumnList<String> jobIds = baseDao.readWithKey(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"stat_job_ids",0);
    	
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
    		
			/*baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "start_count", ""+startVal);
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "end_count",  ""+endVal);
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(), jobId, "job_status", "Inprogress");*/
			baseDao.saveStringValue(ColumnFamily.RECENTVIEWEDRESOURCES.getColumnFamily(),"stat_job_ids", "job_names", runningJobs+","+jobId);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "total_job_count", ""+totalJobCount);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "running_job_count", ""+jobCount);
			baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"stat_job_settings", "indexed_count", ""+endVal);
    		
    		
    		JSONArray resourceList = new JSONArray();
    		try {
	    		for(long i = startVal ; i <= endVal ; i++){
	    			logger.info("contentId : "+ i);
	    			String gooruOid = null;
	    			Rows<String, String> resource = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), "content_id", i,0);
	    			if(resource != null && resource.size() > 0){
    					ColumnList<String> columns = resource.getRowByIndex(0).getColumns();
    					
    					String resourceType = columns.getColumnByName("type_name").getStringValue().equalsIgnoreCase("scollection") ? "scollection" : "resource";

    					gooruOid = columns.getColumnByName("gooru_oid").getStringValue(); 
	    				
    					logger.info("gooruOid : {}",gooruOid);
	    				
	    				if(gooruOid != null){
	    					long insightsView = 0L;
	    					long gooruView = columns.getLongValue("views_count", 0L);
	    					
	    					ColumnList<String> vluesList = baseDao.readWithKeyColumnList(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+gooruOid, columnList,0);
	    					JSONObject resourceObj = new JSONObject();
	    					for(Column<String> detail : vluesList) {
	    						resourceObj.put("gooruOid", gooruOid);
	    				
	    						if(detail.getName().contains("views")){
	    							insightsView = detail.getLongValue();
	    							long balancedView = (gooruView - insightsView);
	    							if(balancedView != 0){
	    								baseDao.increamentCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+gooruOid, "count~views", balancedView);
	    							}
	    							logger.info("Generating resource Object : {}",balancedView);
	    							resourceObj.put("views", (insightsView + balancedView));
	    						}
	    						
	    						if(detail.getName().contains("ratings")){
	    							resourceObj.put("ratings", detail.getLongValue());
	    						}
	    						resourceObj.put("resourceType", resourceType);
	    					}
	    					resourceList.put(resourceObj);
	    				}
    				
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
		ColumnList<String> settings = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "bal_stat_job_settings",0);
		for (long startDate = Long.parseLong(settings.getStringValue("last_updated_time", null)) ; startDate <= Long.parseLong(minuteDateFormatter.format(new Date()));) {
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
  		MutationBatch m = getConnectionProvider().getAwsKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
  		MutationBatch m2 = getConnectionProvider().getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
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
				 baseDao.saveBulkList(ColumnFamily.DIMEVENTS.getColumnFamily(),key,records);
		return true;
    	}
    	return false;
    }
    
	public boolean validateSchedular(String ipAddress) {

		try {
			//ColumnList<String> columnList = configSettings.getColumnList("schedular~ip");
			ColumnList<String> columnList = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"schedular~ip",0);
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
					 if(localClassCache.get("ORG~"+userUid) != null){
						 organizationUid = String.valueOf(localClassCache.get("ORG~"+userUid));
					 }else{
					 Rows<String, String> userDetails = baseDao.readIndexedColumn(ColumnFamily.DIMUSER.getColumnFamily(), "gooru_uid", userUid,0);
	   					for(Row<String, String> userDetail : userDetails){
	   						organizationUid = userDetail.getColumns().getStringValue("organization_uid", null);
	   						localClassCache.put("ORG~"+userUid,organizationUid,DEFAULTEXPIRETIME);
	   					}
	   					
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
        		long parentStartTime = existingParentRecord.getLongValue("start_time", null);
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

    
    public Map<String,String> getKafkaProperty(String propertyName){
    	return kafkaConfigurationCache.get(propertyName);    	
    }
}

  
