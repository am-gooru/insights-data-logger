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
package org.kafka.event.microaggregator.core;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.kafka.event.microaggregator.dao.CounterDetailsDAOCassandraImpl;
import org.kafka.event.microaggregator.dao.RealTimeOperationConfigDAOImpl;
import org.kafka.event.microaggregator.model.EventObject;
import org.kafka.event.microaggregator.model.JSONDeserializer;
import org.kafka.event.microaggregator.model.TypeConverter;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.kafka.event.microaggregator.dao.ActivityStreamDAOCassandraImpl;
import org.kafka.event.microaggregator.dao.AggregationDAO;
import org.kafka.event.microaggregator.dao.AggregationDAOImpl;
import org.kafka.event.microaggregator.dao.DimUserDAOCassandraImpl;
import org.kafka.event.microaggregator.dao.EventDetailDAOCassandraImpl;
import org.kafka.event.microaggregator.dao.LiveDashBoardDAOImpl;
import org.kafka.event.microaggregator.dao.MicroAggregationDAO;
import org.kafka.event.microaggregator.dao.MicroAggregationDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;

import flexjson.JSONSerializer;


public class MicroAggregationLoader implements Constants{

    private static final Logger logger = LoggerFactory.getLogger(MicroAggregationLoader.class);
   
    private Keyspace cassandraKeyspace;
    
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;

    private CassandraConnectionProvider connectionProvider;
        
    private CounterDetailsDAOCassandraImpl counterDetailsDao;
    
    private LiveDashBoardDAOImpl liveDashboardDAOImpl;
    
    private EventDetailDAOCassandraImpl eventDetailDao;

    private DimUserDAOCassandraImpl dimUser;
    
    private ActivityStreamDAOCassandraImpl activityStreamDao;
    
    private RealTimeOperationConfigDAOImpl realTimeOperation;
    
    private AggregationDAOImpl aggregationDAO;
    
    public static  Map<String,String> realTimeOperators;
    
    private MicroAggregatorProducer microAggregator;
    
    private MicroAggregationDAOImpl microAggregationDAOImpl;
    
	private static Map<String, Map<String, String>> kafkaConfigurationCache;
    
    private Gson gson = new Gson();
    /**
     * Get Kafka properties from Environment
     */
    public MicroAggregationLoader() {
        this(null);
        
        kafkaConfigurationCache = new HashMap<String, Map<String, String>>();
		Set<String> rowKeyProperties = new HashSet<String>();
		rowKeyProperties.add("kafka~microaggregator~producer");
		rowKeyProperties.add("kafka~microaggregator~consumer");
		rowKeyProperties.add("kafka~microaggregator~producer");
		rowKeyProperties.add("kafka~logwritter~consumer");
		rowKeyProperties.add("kafka~logwritter~producer");
		Rows<String, String> result = aggregationDAO.readRows(CONFIG_SETTINGS, rowKeyProperties, new ArrayList<String>()).getResult();
		for (Row<String, String> row : result) {
			Map<String, String> properties = new HashMap<String, String>();
			for (Column<String> column : row.getColumns()) {
				properties.put(column.getName(), column.getStringValue());
			}
			kafkaConfigurationCache.put(row.getKey(), properties);
		}
    }

    public MicroAggregationLoader(Map<String, String> configOptionsMap) {
        init(configOptionsMap);
        this.gson = new Gson();
    }

    /**
     * *
     * @param configOptionsMap
     * Initialize Coulumn Family
     */
    
    private void init(Map<String, String> configOptionsMap) {
    	
        this.setConnectionProvider(new CassandraConnectionProvider());
        this.getConnectionProvider().init(configOptionsMap);
        this.aggregationDAO  = new AggregationDAOImpl(getConnectionProvider());
        this.counterDetailsDao = new CounterDetailsDAOCassandraImpl(getConnectionProvider());
        this.realTimeOperation = new RealTimeOperationConfigDAOImpl(getConnectionProvider());
        this.liveDashboardDAOImpl = new LiveDashBoardDAOImpl(getConnectionProvider());
        this.eventDetailDao = new EventDetailDAOCassandraImpl(getConnectionProvider());
        this.dimUser = new DimUserDAOCassandraImpl(getConnectionProvider());
        this.aggregationDAO = new AggregationDAOImpl(getConnectionProvider()); 
        this.activityStreamDao = new ActivityStreamDAOCassandraImpl(getConnectionProvider());
        realTimeOperators = realTimeOperation.getOperators();

    }
    
    @Async
    public void microRealTimeAggregation(String eventJSON) throws JSONException{
    	
    	logger.info("Enterssssssssssssss");
    	
    	
    	String userUid = null;
    	String organizationUid = null;
    	JsonObject eventObj = new JsonParser().parse(eventJSON).getAsJsonObject();
    	EventObject eventObject = gson.fromJson(eventObj, EventObject.class);
    	Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObject);
    	
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
    	 logger.info("eventName : {} -  eventId : {} ",eventObject.getEventName(),eventObject.getEventId() );
    /*	String aggregatorJson = realTimeOperators.get(eventMap.get("eventName"));
    	counterDetailsDao.realTimeMetrics(eventMap, aggregatorJson);*/
		
    	try {
			liveDashboardDAOImpl.findDifferenceInCount(eventMap);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    }
    
    
 public void updateActivityStream(String eventJSON) throws JSONException {
    	
    	JsonObject eventObj = new JsonParser().parse(eventJSON).getAsJsonObject();
    	EventObject rootEventObject = gson.fromJson(eventObj, EventObject.class);
    	String eventId = rootEventObject.getEventId();
    	if (eventId != null){

	    	Map<String,String> rawMap = new HashMap<String, String>();
		    String apiKey = null;
	    	String userName = null;
	    	String dateId = null;
	    	String userUid = null;
	    	String contentGooruId = null;
	    	String parentGooruId = null;
	    	String organizationUid = null;
	    	String score = null;
            String eventType = null;
            int[] attempStatus;
	    	Date endDate = new Date();
	    	
	    	ColumnList<String> activityRow = eventDetailDao.readEventDetail(eventId);	
	    	String fields = activityRow.getStringValue(FIELDS, null);
	    	
	    	if(activityRow != null & activityRow.size() > 0){
	    	if (fields != null){
		    		JsonObject rawJson = new JsonParser().parse(fields).getAsJsonObject();
		        	EventObject eventObject = gson.fromJson(rawJson, EventObject.class);
			    	rawMap = JSONDeserializer.deserializeEventObject(eventObject);
	    	} else {
	    		logger.error("Fields is empty or invalid");
	    	}
	         	
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
		    this.updateActivityCompletion(userUid, activityRow, eventId, timeMap);

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
		    if(rawMap.get(TYPE) != null && (rawMap.get(TYPE).equalsIgnoreCase(STOP) || (eventType != null && ("completed-event".equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType)))) && rawMap.get(MODE).equalsIgnoreCase(STUDY) && rawMap.get(RESOURCETYPE).equalsIgnoreCase(QUESTION)) {
		    	if(rawMap != null && rawMap.get(SCORE) != null && rawMap.get(SCORE).toString() != null && rawMap.get(SESSION) != null && rawMap.get(SESSION).toString() != null){
			    	score = rawMap.get(SCORE).toString();
			    	eventMap.put("score", score);
			    	eventMap.put("session_id", rawMap.get(SESSION).toString());
					attempStatus = TypeConverter.stringToIntArray(rawMap.get(ATTMPTSTATUS)) ;
					if(attempStatus.length > 0){
						eventMap.put("first_attempt_status", attempStatus[0]);
					}
    				String answerStatus = null;
					if(attempStatus.length == 0){
    					answerStatus = LoaderConstants.SKIPPED.getName();
        			}else {
    	    			if(attempStatus[0] == 1){
    	    				answerStatus = LoaderConstants.CORRECT.getName();
    	    			}else if(attempStatus[0] == 0){
    	    				answerStatus = LoaderConstants.INCORRECT.getName();
    	    			}
        			}
    				eventMap.put("answer_status", answerStatus);
			    }
		    } else if (rawMap.get(TYPE) != null && (rawMap.get(TYPE).equalsIgnoreCase(STOP) || (eventType != null && ("completed-event".equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType)))) && rawMap.get(MODE).equalsIgnoreCase(STUDY)) {
		    	if(rawMap != null && rawMap.get(SCORE) != null && rawMap.get(SCORE).toString() != null && rawMap.get(SESSION) != null && rawMap.get(SESSION).toString() != null){
			    	score = rawMap.get(SCORE).toString();
			    	eventMap.put("score", score);
			    	eventMap.put("session_id", rawMap.get(SESSION).toString());
			    }
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
	    	eventMap.put("api_key", apiKey);
	    	eventMap.put("organization_uid", organizationUid);
	    	eventMap.put("date_time", dateId);
	
	    	activityMap.put("activity", new JSONSerializer().serialize(eventMap));
	    	
	    	activityStreamDao.saveActivity(activityMap);
	    	} else {
	    		logger.info("Entry is not available for this key in EventDetailCF {}", eventId);
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

        if (!StringUtils.isEmpty(eventType)) {
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
    
    /*
     * This will run for every minute
     */
	public void staticAggregation(String eventJson){
		try{
			logger.info("Event Json:"+eventJson);
		JSONObject jsonObject = new JSONObject(eventJson);
		String startTime = null;
		String endTime = null;
		if(jsonObject.has("startTime")){
			startTime = jsonObject.get("startTime") != null ? jsonObject.get("startTime").toString() : null;
		}
		if(jsonObject.has("endTime")){
			endTime = jsonObject.get("endTime") != null ? jsonObject.get("endTime").toString() : null;
		}
		aggregationDAO.startStaticAggregation(startTime,endTime);
		}catch(Exception e){
			e.printStackTrace();
		}
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
    
    public Map<String, String> getKafkaProperty(String name) {
		return kafkaConfigurationCache.get(name);
	}
    
}

