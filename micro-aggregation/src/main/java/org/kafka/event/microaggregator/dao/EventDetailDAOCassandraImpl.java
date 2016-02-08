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
import org.kafka.event.microaggregator.model.Event;
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
    
    public String  saveEventt(Event event){
    	
    	String key = null;
    	if(event.getEventId() == null){
    		UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    		key = eventKeyUUID.toString();
    	}else{
    		key	= event.getEventId(); 
    	}
    
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	
        m.withRow(eventDetailsCF, key)
                .putColumnIfNotNull("start_time", event.getStartTime(), null)
                .putColumnIfNotNull("end_time", event.getEndTime(),null)
                .putColumnIfNotNull("fields", event.getFields(),null)
                .putColumnIfNotNull("time_spent_in_millis",event.getTimeInMillSec(),null)
                .putColumnIfNotNull("content_gooru_oid",event.getContentGooruId(),null)
                .putColumnIfNotNull("parent_gooru_oid",event.getParentGooruId(),null)
                .putColumnIfNotNull("event_name", event.getEventName(),null)
                .putColumnIfNotNull("session",event.getSession().toString(),null)
                .putColumnIfNotNull("metrics",event.getMetrics().toString(),null)
                .putColumnIfNotNull("pay_load_object",event.getPayLoadObject().toString(),null)
                .putColumnIfNotNull("user",event.getUser().toString(),null)
                .putColumnIfNotNull("context",event.getContext().toString(),null)
        		.putColumnIfNotNull("event_type",event.getEventType().toString(),null)
        		.putColumnIfNotNull("organization_uid",event.getOrganizationUid(),null)
        		.putColumnIfNotNull("parent_event_id",event.getParentEventId(), null);
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
