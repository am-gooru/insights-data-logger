/*******************************************************************************
 * EventDetailDAOCassandraImpl.java
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
package org.logger.event.cassandra.loader.dao;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class EventDetailDAOCassandraImpl extends BaseDAOCassandraImpl implements EventDetailDAO {

    private static final Logger logger = LoggerFactory.getLogger(EventDetailDAOCassandraImpl.class);
    private final ColumnFamily<String, String> eventDetailsCF;
    private static final String CF_EVENT_DETAILS_NAME = "event_detail";
   
    private final ColumnFamily<String, String> counterDetailsCF;
    private static final String CF_COUNTER_DETAILS_NAME = "view_count";
    
    
    public EventDetailDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        eventDetailsCF = new ColumnFamily<String, String>(
                CF_EVENT_DETAILS_NAME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
        counterDetailsCF = new ColumnFamily<String, String>(
                CF_COUNTER_DETAILS_NAME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
    }

    /**
     * @param eventData,appOid
     *          eventData is the Object and appOid to save
     * @throws ConnectionException
     *             if the host is unavailable 
     */
    
    @Override
    public String saveEvent(EventData eventData,String appOid) {
    	String key = null;
    	if(eventData.getEventId() == null){
    		UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    		key = eventKeyUUID.toString();
    	}else{
    		key	= eventData.getEventId(); 
    	}
    	if(appOid == null){
    		appOid = "GLP";
    	}
    	String gooruOid = eventData.getContentGooruId();
    	if(gooruOid == null){
    		gooruOid = eventData.getGooruOId();
    	}
    	if(gooruOid == null){
    		gooruOid = eventData.getGooruId();
    	}
    	if((gooruOid == null || gooruOid.isEmpty()) && eventData.getResourceId() != null){
    		gooruOid = eventData.getResourceId();
    	}
    	String eventValue = eventData.getQuery();
    	if(eventValue == null){
    		eventValue = "NA";
    	}
    	String parentGooruOid = eventData.getParentGooruId();
    	if((parentGooruOid == null || parentGooruOid.isEmpty()) && eventData.getCollectionId() != null){
    		parentGooruOid = eventData.getCollectionId();
    	}
    	if(parentGooruOid == null || parentGooruOid.isEmpty()){
    		parentGooruOid = "NA";
    	}
    	if(gooruOid == null || gooruOid.isEmpty()){
    		gooruOid = "NA";
    	}	
    	String organizationUid  = eventData.getOrganizationUid();
    	if(organizationUid == null){
    		organizationUid = "NA";
    	}
    	String GooruUId = eventData.getGooruUId();
logger.info("GooruUId : {}",GooruUId);
    	String appUid = appOid+"~"+gooruOid;    	
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddkkmm");
        String date = ft.format(dNow).toString();

        String trySeq= null;
        String attemptStatus= null;
        String answereIds= null;
        
        if(eventData.getAttemptTrySequence() !=null){
        	trySeq = eventData.getAttemptTrySequence().toString();
        }
        if( eventData.getAttemptStatus() != null){
        	attemptStatus = eventData.getAttemptStatus().toString();
        }
        if(eventData.getAnswerId() != null){
        	answereIds = eventData.getAnswerId().toString();
        }
        // Inserting data
        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
             
        m.withRow(eventDetailsCF, key)
                .putColumn("date_time", date, null)
                .putColumnIfNotNull("start_time", eventData.getStartTime(), null)
                .putColumnIfNotNull("user_ip", eventData.getUserIp(), null)
                .putColumnIfNotNull("fields", eventData.getFields(), null)
                .putColumnIfNotNull("user_agent", eventData.getUserAgent(), null)
                .putColumnIfNotNull("session_token",eventData.getSessionToken(), null)
                .putColumnIfNotNull("end_time", eventData.getEndTime(), null)
                .putColumnIfNotNull("content_gooru_oid", gooruOid, null)
                .putColumnIfNotNull("parent_gooru_oid",parentGooruOid, null)
                .putColumnIfNotNull("event_name", eventData.getEventName(), null)
                .putColumnIfNotNull("api_key", eventData.getApiKey(), null)
                .putColumnIfNotNull("time_spent_in_millis", eventData.getTimeInMillSec())
                .putColumnIfNotNull("event_source", eventData.getEventSource())
                .putColumnIfNotNull("content_id", eventData.getContentId(), null)
                .putColumnIfNotNull("event_value", eventValue, null)
                .putColumnIfNotNull("gooru_uid", GooruUId,null)
                .putColumnIfNotNull("event_type", eventData.getEventType(),null)
                .putColumnIfNotNull("user_id", eventData.getUserId(),null)
                .putColumnIfNotNull("organization_uid", organizationUid)
                .putColumnIfNotNull("app_oid", appOid, null)
                .putColumnIfNotNull("app_uid", appUid, null)
		        .putColumnIfNotNull("city", eventData.getCity(), null)
		        .putColumnIfNotNull("state", eventData.getState(), null)
		        .putColumnIfNotNull("attempt_number_of_try_sequence", eventData.getAttemptNumberOfTrySequence(), null)
		        .putColumnIfNotNull("attempt_first_status", eventData.getAttemptFirstStatus(), null)
		        .putColumnIfNotNull("answer_first_id", eventData.getAnswerFirstId(), null)
		        .putColumnIfNotNull("attempt_try_sequence", trySeq, null)
		        .putColumnIfNotNull("attempt_status", attemptStatus, null)
		        .putColumnIfNotNull("answer_ids", answereIds, null)
		        .putColumnIfNotNull("country",eventData.getCountry(), null)
		        .putColumnIfNotNull("contextInfo",eventData.getContextInfo(), null)
		        .putColumnIfNotNull("collaboratorIds",eventData.getCollaboratorIds(), null)
		        .putColumnIfNotNull("mobileData",eventData.isMobileData(), null)
		        .putColumnIfNotNull("hintId",eventData.getHintId(), null)
		        .putColumnIfNotNull("open_ended_text",eventData.getOpenEndedText(), null)
		        .putColumnIfNotNull("parent_event_id",eventData.getParentEventId(), null);
        
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra - JSON - ", e);
            return null;
        }
        return key;
    }
    
    public Map<String,String> generateStringMap(EventData eventData,String appOid) {
    	Map<String,String> eventDataMap = new LinkedHashMap<String, String>();

    	
    	if(appOid == null){
    		appOid = "GLP";
    	}
    	String gooruOid = eventData.getContentGooruId();
    	if(gooruOid == null){
    		gooruOid = eventData.getGooruOId();
    	}
    	if(gooruOid == null){
    		gooruOid = eventData.getGooruId();
    	}
    	if((gooruOid == null || gooruOid.isEmpty()) && eventData.getResourceId() != null){
    		gooruOid = eventData.getResourceId();
    	}
    	String eventValue = eventData.getQuery();
    	if(eventValue == null){
    		eventValue = "NA";
    	}
    	String parentGooruOid = eventData.getParentGooruId();
    	if((parentGooruOid == null || parentGooruOid.isEmpty()) && eventData.getCollectionId() != null){
    		parentGooruOid = eventData.getCollectionId();
    	}
    	if(parentGooruOid == null || parentGooruOid.isEmpty()){
    		parentGooruOid = "NA";
    	}
    	if(gooruOid == null || gooruOid.isEmpty()){
    		gooruOid = "NA";
    	}	
    	String organizationUid  = eventData.getOrganizationUid();
    	if(organizationUid == null){
    		organizationUid = "NA";
    	}
    	String GooruUId = eventData.getGooruUId();
    	String appUid = appOid+"~"+gooruOid;    	
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddkkmm");
        String date = ft.format(dNow).toString();

        String trySeq= null;
        String attemptStatus= null;
        String answereIds= null;
        
        if(eventData.getAttemptTrySequence() !=null){
        	trySeq = eventData.getAttemptTrySequence().toString();
        }
        if( eventData.getAttemptStatus() != null){
        	attemptStatus = eventData.getAttemptStatus().toString();
        }
        if(eventData.getAnswerId() != null){
        	answereIds = eventData.getAnswerId().toString();
        }
  
                eventDataMap.put("date_time", date);
                
                eventDataMap.put("user_ip", eventData.getUserIp());
                eventDataMap.put("fields", eventData.getFields());
                eventDataMap.put("user_agent", eventData.getUserAgent());
                eventDataMap.put("session_token",eventData.getSessionToken());
                eventDataMap.put("content_gooru_oid", gooruOid);
                eventDataMap.put("parent_gooru_oid",parentGooruOid);
                eventDataMap.put("event_name", eventData.getEventName());
                eventDataMap.put("api_key", eventData.getApiKey());
                eventDataMap.put("event_source", eventData.getEventSource());
                eventDataMap.put("content_id", eventData.getContentId());
                eventDataMap.put("event_value", eventValue);
                eventDataMap.put("gooru_uid", GooruUId);
                eventDataMap.put("event_type", eventData.getEventType());
                eventDataMap.put("user_id", eventData.getUserId());
                eventDataMap.put("organization_uid", organizationUid);
                eventDataMap.put("app_oid", appOid);
                eventDataMap.put("app_uid", appUid);
		        eventDataMap.put("city", eventData.getCity());
		        eventDataMap.put("state", eventData.getState());
		        eventDataMap.put("attempt_first_status", eventData.getAttemptFirstStatus());
		        eventDataMap.put("attempt_try_sequence", trySeq);
		        eventDataMap.put("attempt_status", attemptStatus);
		        eventDataMap.put("answer_ids", answereIds);
		        eventDataMap.put("country",eventData.getCountry());
		        eventDataMap.put("contextInfo",eventData.getContextInfo());
		        eventDataMap.put("collaboratorIds",eventData.getCollaboratorIds());
		        eventDataMap.put("mobileData",""+eventData.isMobileData());
		        eventDataMap.put("hintId",""+eventData.getHintId());
		        eventDataMap.put("open_ended_text",eventData.getOpenEndedText());
		        eventDataMap.put("parent_event_id",eventData.getParentEventId());
        
       
        return eventDataMap;
    }
    
    public Map<String,Long> generateLongMap(EventData eventData,String appOid) {
    	Map<String,Long> eventDataMap = new LinkedHashMap<String, Long>();
    	eventDataMap.put("start_time", eventData.getStartTime());
    	eventDataMap.put("end_time", eventData.getEndTime());
    	eventDataMap.put("time_spent_in_millis", eventData.getTimeInMillSec());
    	return eventDataMap;
    }
    @Async
    public String  saveEventObject(EventObject eventObject){
    	
    	String key = null;
    	if(eventObject.getEventId() == null){
    		UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    		key = eventKeyUUID.toString();
    	}else{
    		key	= eventObject.getEventId(); 
    	}
    
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	
        m.withRow(eventDetailsCF, key)
                .putColumnIfNotNull("start_time", eventObject.getStartTime(), null)
                .putColumnIfNotNull("end_time", eventObject.getEndTime(),null)
                .putColumnIfNotNull("fields", eventObject.getFields(),null)
                .putColumnIfNotNull("time_spent_in_millis",eventObject.getTimeInMillSec(),null)
                .putColumnIfNotNull("content_gooru_oid",eventObject.getContentGooruId(),null)
                .putColumnIfNotNull("parent_gooru_oid",eventObject.getParentGooruId(),null)
                .putColumnIfNotNull("event_name", eventObject.getEventName(),null)
                .putColumnIfNotNull("session",eventObject.getSession(),null)
                .putColumnIfNotNull("metrics",eventObject.getMetrics(),null)
                .putColumnIfNotNull("pay_load_object",eventObject.getPayLoadObject(),null)
                .putColumnIfNotNull("user",eventObject.getUser(),null)
                .putColumnIfNotNull("context",eventObject.getContext(),null)
        		.putColumnIfNotNull("event_type",eventObject.getEventType(),null)
        		.putColumnIfNotNull("organization_uid",eventObject.getOrganizationUid(),null)
        		.putColumnIfNotNull("parent_event_id",eventObject.getParentEventId(), null);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting Event Object to cassandra - JSON - ", e);
            return null;
        }
		return key;
                
                
    }
    /**
     * @param key,endTime,timeSpent
     *		save time spent for the particular resource in given time. 
     * @throws ConnectionException
     *             if the host is unavailable
     */
    public void updateParentId(String key,Long endTime,Long timeSpent ){
    
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        m.withRow(eventDetailsCF, key)
                .putColumnIfNotNull("end_time", endTime, null)
                .putColumnIfNotNull("time_spent_in_millis", timeSpent,null);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra  ");
        }
    
    	
    }
    
    /**
     * @param eventKey
     *           eventKey to lookup.
     * @return ColumnList<String> with the eventData Object
     * @throws ConnectionException
     *             if the host is unavailable
     */
    public ColumnList<String> readEventDetail(String eventKey){
    	
    	ColumnList<String> eventDetail = null;
    	try {
			 eventDetail = getKeyspace().prepareQuery(eventDetailsCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(eventKey).execute().getResult();
		} catch (ConnectionException e) {
			
			logger.info("Error while retieveing data : {}" ,e);
		}
    	
    	return eventDetail;
    }

    public Rows<String,String> readEventDetailList(Collection<String> eventKey){
    	
    	Rows<String, String> eventDetail = null;
    	try {
			 eventDetail = getKeyspace().prepareQuery(eventDetailsCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKeySlice(eventKey)
					 .execute().getResult();
		} catch (ConnectionException e) {
			
			logger.info("Error while retieveing data : {}" ,e);
		}
    	
    	return eventDetail;
    }
	

    /**
     * @param apiKey,rowsToRead
     *           apiKey to lookup and rowsToRead for number of rows to return.
     * @return Rows<String, String> with the eventData Object
     * @throws ConnectionException
     *             if the host is unavailable
     */
    
	public Rows<String, String> readLastNrows(String apiKey, Integer rowsToRead) {
		Rows<String, String> eventDetail = null;
    	try {
			 eventDetail = getKeyspace().prepareQuery(eventDetailsCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 	   .searchWithIndex().autoPaginateRows(true).setRowLimit(rowsToRead.intValue()).addExpression().whereColumn("api_key")
					 	   .equals().value(apiKey).execute().getResult();
		} catch (ConnectionException e) {
			
			logger.info("Error while retieveing data : {}" ,e);
		}
    	
    	return eventDetail;
	}
	

    /**
     * @param key,city,state,country
     *           save locations for the user key.
     * @throws ConnectionException
     *             if the host is unavailable
     */

	
	public String saveGeoLocation(String key, String  city, String  state, String country){
        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        
        m.withRow(eventDetailsCF, key)
		        .putColumnIfNotNull("city", city, null)
		        .putColumnIfNotNull("state", state, null)
		        .putColumnIfNotNull("country", country, null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info(
                    "Error while inserting geo-location to cassandra - JSON - "
                    + (key
                    + city
                    + state
                    + country ), e);
            return null;
        }
        return key;
	}
	
}
