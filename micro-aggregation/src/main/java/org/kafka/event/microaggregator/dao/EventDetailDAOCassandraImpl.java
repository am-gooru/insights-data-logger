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
package org.kafka.event.microaggregator.dao;
import java.util.UUID;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.model.EventObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
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
                .putColumnIfNotNull("session",eventObject.getSession().toString(),null)
                .putColumnIfNotNull("metrics",eventObject.getMetrics().toString(),null)
                .putColumnIfNotNull("pay_load_object",eventObject.getPayLoadObject().toString(),null)
                .putColumnIfNotNull("user",eventObject.getUser().toString(),null)
                .putColumnIfNotNull("context",eventObject.getContext().toString(),null)
        		.putColumnIfNotNull("event_type",eventObject.getEventType().toString(),null)
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
     * @param eventKey
     *           eventKey to lookup.
     * @return ColumnList<String> with the eventData Object
     * @throws ConnectionException
     *             if the host is unavailable
     */
    public ColumnList<String> readEventDetail(String eventKey){
    	
    	ColumnList<String> eventDetail = null;
    	try {
			 eventDetail = getKeyspace().prepareQuery(eventDetailsCF)
			 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 .getKey(eventKey)
			 .execute().getResult();
		} catch (ConnectionException e) {
			
			logger.info("Error while retieveing data : {}" ,e);
		}
    	
    	return eventDetail;
    }

}
