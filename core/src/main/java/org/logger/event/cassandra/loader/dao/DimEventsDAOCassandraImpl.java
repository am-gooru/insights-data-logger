/*******************************************************************************
 * DimEventsDAOCassandraImpl.java
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

import java.util.HashMap;
import java.util.UUID;

import org.ednovo.data.model.EventData;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Caching;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class DimEventsDAOCassandraImpl extends BaseDAOCassandraImpl implements DimEventsDAO{

    private static final Logger logger = LoggerFactory.getLogger(DimEventsDAOCassandraImpl.class);
    
    private final ColumnFamily<String, String> dimEventCF;
    
    private static final String CF_DIM_EVENTS = "dim_events";

	public DimEventsDAOCassandraImpl(
			CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		dimEventCF = new ColumnFamily<String, String>(
				CF_DIM_EVENTS, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
	}
	
	public ColumnList<String> readEventName(EventData eventData) throws ConnectionException{

		ColumnList<String> existingEventRecord = getKeyspace().prepareQuery(dimEventCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(eventData.getEventName()).execute().getResult();
		
		return existingEventRecord;
		
	}

	public boolean isEventNameExists(String eventName) throws ConnectionException{

		return getKeyspace().prepareQuery(dimEventCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(eventName).execute().getResult().isEmpty();
	}
	
	/**
     * @param eventName
     *            eventName to lookup.
     * @return String with the event id.
     * @throws ConnectionException
     *             if host is unavailable
     */
	@Caching
	public String getEventId(String eventName) {

		ColumnList<String> existingEventRecord = null;
		try {
			existingEventRecord = getKeyspace().prepareQuery(dimEventCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(eventName).execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		return existingEventRecord.getStringValue("event_id", null);
		
	}
	
	/**
     * @return HashMap<String, String> with event name and event id.
     * @throws ConnectionException
     *             if host is unavailable
     */
	public HashMap<String, String> readAllEventNames(){
		Rows<String, String> allEvents = null;
		HashMap<String, String> events = new HashMap<String, String>();
		try {
			allEvents = getKeyspace().prepareQuery(dimEventCF)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getAllRows()
					.withColumnRange(new RangeBuilder().setMaxSize(10).build())
			        .setExceptionCallback(new ExceptionCallback() {
			             @Override
			             public boolean onException(ConnectionException e) {
			                 try {
			                     Thread.sleep(1000);
			                 } catch (InterruptedException e1) {
			                 }
			                 return true;
			             }})
			        .execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		for (Row<String, String> row : allEvents) {
			events.put(row.getKey(), row.getColumns().getStringValue("event_id", null));
		}
		return events;
	}

	/**
     * @param name
     *            is new event name 
     * @throws ConnectionException
     *             if host is unavailable
     */
	public boolean saveEventName(String name) {
		int lastEventId=1000;
		try {
			ColumnList<String> existingEventRecord = getKeyspace().prepareQuery(dimEventCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey("update-event-id").execute().getResult();
			lastEventId = Integer.parseInt(existingEventRecord.getColumnByName("event_id").getStringValue());
		} catch (ConnectionException e1) {
			logger.info("unable to get existing eventId", e1);
			e1.printStackTrace();
			return false;
		}
	    MutationBatch eventIdMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    eventIdMutation.withRow(dimEventCF, name)
	    .putColumn("event_id", String.valueOf(lastEventId), null);
	    lastEventId++;
	    try {
	    	eventIdMutation.execute();
	    	eventIdMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		    eventIdMutation.withRow(dimEventCF, "update-event-id")
		    .putColumn("event_id", String.valueOf(lastEventId), null);
		    eventIdMutation.execute();
	    } catch (ConnectionException e) {
	        logger.info("Error while inserting event data to cassandra", e);
	        return false;
	    }
	return true;
	}
}
