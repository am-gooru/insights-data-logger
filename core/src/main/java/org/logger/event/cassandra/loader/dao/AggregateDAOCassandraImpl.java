/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   AggregateDAOCassandraImpl.java
 *   event-api-stable-1.2
 *   
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *  
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *  
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.logger.event.cassandra.loader.dao;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.logging.Level;

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class AggregateDAOCassandraImpl extends BaseDAOCassandraImpl implements AggregateStagingDAO{

    private static final Logger logger = LoggerFactory.getLogger(AggregateDAOCassandraImpl.class);
    private final ColumnFamily<String, String> aggEventResourcesStagingCF;
    private final ColumnFamily<String, String> eventTimelineCF;
    private static final String CF_AGG_STAGING = "stging_event_resource_user";
    private static final String CF_TIME_LINE = "event_timeline";
    
	public AggregateDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		aggEventResourcesStagingCF = new ColumnFamily<String, String>(
				CF_AGG_STAGING, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
		
		eventTimelineCF = new ColumnFamily<String, String>(CF_TIME_LINE, 
				StringSerializer.get(), 
				StringSerializer.get());
	}

	 /**
     * @param stagingEvents
     *           raw data from staging table 
     * @throws ConnectionException
     *             if there is an error while inserting a record or unavailable host 
     */
	public void saveAggregation(HashMap<String, String> stagingEvents){
		
		 MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		 Long eventId = 0L;
		 Long minuteId = 0L;
		 Long hourId = 0L;
		 Long dateId = 0L;
		 Long timeSpentMs = 0L;
		 
		 if(stagingEvents.get("eventId") != null){
			 eventId = Long.parseLong(stagingEvents.get("eventId"));
		 }
		
		 if(stagingEvents.get("minuteId") != null){
			 minuteId = Long.parseLong(stagingEvents.get("minuteId"));
		 }
		
		 if(stagingEvents.get("hourId") != null){
			 hourId = Long.parseLong(stagingEvents.get("hourId"));
		 }
		
		 if(stagingEvents.get("dateId") != null){
			 dateId = Long.parseLong(stagingEvents.get("dateId"));
		 }
		 if(stagingEvents.get("timeSpentInMillis") != null){
			 timeSpentMs = Long.parseLong(stagingEvents.get("timeSpentInMillis"));
		 }
		
		 
        m.withRow(aggEventResourcesStagingCF, stagingEvents.get("keys").toString())
        .putColumnIfNotNull("minute_id",minuteId)
        .putColumnIfNotNull("hour_id", hourId)
        .putColumnIfNotNull("date_id", dateId)
        .putColumnIfNotNull("event_id",eventId)
        .putColumnIfNotNull("user_uid", stagingEvents.get("userUid") ,null)
        .putColumnIfNotNull("gooru_oid", stagingEvents.get("contentGooruOid"),null)
        .putColumnIfNotNull("parent_gooru_oid", stagingEvents.get("parentGooruOid"),null)
        .putColumnIfNotNull("total_timespent_ms", timeSpentMs)
        .putColumnIfNotNull("organization_uid", stagingEvents.get("organizationUid"),null)
        .putColumnIfNotNull("event_value", stagingEvents.get("eventValue"),null)
        .putColumnIfNotNull("resource_type", stagingEvents.get("resourceType"),null)
        .putColumnIfNotNull("app_key", stagingEvents.get("appOid"),null)
        .putColumnIfNotNull("app_id", stagingEvents.get("appUid"),null)
        .putColumnIfNotNull("city", stagingEvents.get("city"), null)
        .putColumnIfNotNull("state",stagingEvents.get("state"), null)
        .putColumnIfNotNull("attempt_number_of_try_sequence", stagingEvents.get("attempt_number_of_try_sequence"), null)
        .putColumnIfNotNull("attempt_first_status", stagingEvents.get("attempt_first_status"), null)
        .putColumnIfNotNull("answer_first_id", stagingEvents.get("answer_first_id"), null)
        .putColumnIfNotNull("attempt_try_sequence",stagingEvents.get("attempt_try_sequence"), null)
        .putColumnIfNotNull("attempt_status", stagingEvents.get("attempt_status"), null)
        .putColumnIfNotNull("answer_ids",stagingEvents.get("answer_ids"), null)
        .putColumnIfNotNull("country",stagingEvents.get("country"), null)
        .putColumnIfNotNull("open_ended_text",stagingEvents.get("open_ended_text"), null);
        
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra ", e);
            
        }

	}
	 /**
     * @param timeId
     *            time id to lookup
     * @return ColumnList<String> with event data in particular hour
     */
	public ColumnList<String> readEventName(String timeId) throws ConnectionException{

		ColumnList<String> hourlyEventData = getKeyspace().prepareQuery(aggEventResourcesStagingCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(timeId).execute().getResult();
		
		return hourlyEventData;
		
	}
	
	/**
     * @throws OperationException
     * @throws ConnectionException
     *             if host is unavailable
     */
	public void  deleteAll(){
			try {
				getKeyspace().truncateColumnFamily(aggEventResourcesStagingCF);
			} catch (OperationException e) {
				 logger.info("Error while deleting rows : {} ",e);
			} catch (ConnectionException e) {
				logger.info("Error while connecting columnFamily : {} ",e);
			}
    	
	}
	

	/**
	 * @param timeStampMinuteStart,timeStampMinuteStop,dryRun
	 * 		start and stop time stamp look-up and dryRun staging record for delete operation 
     * @throws ConnectionException
     *             if host is unavailable
     */
	public void deleteEventsGivenTimeline(String timeStampMinuteStart, String timeStampMinuteStop, final boolean dryRun){
        try {
            final long timeStampStart = Long.parseLong(timeStampMinuteStart);
            final long timeStampStop = Long.parseLong(timeStampMinuteStop);

            getKeyspace().prepareQuery(eventTimelineCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getAllRows()
                    .setRowLimit(100) // Read in blocks of 100
                    .setRepeatLastToken(false)
                    .withColumnRange(new RangeBuilder().setLimit(0).build())
                    .executeWithCallback(new RowCallback<String, String>() {
                @Override
                public void success(Rows<String, String> rows) {
                    final Collection<String> blockKeys = rows.getKeys();
                    for (String rowKey : blockKeys) {
                        final long keyTimeStamp = Long.parseLong(rowKey);
                        if (!(keyTimeStamp >= timeStampStart && keyTimeStamp <= timeStampStop)) {
                            // Skip row
                            return;
                        }
                        logger.info("Going to delete eventTimeline for {}", rowKey);
                        if (dryRun) {
//                            continue;
                        }
                        ColumnList<String> columns;
                        try {
                            RowQuery<String, String> query = getKeyspace()
                                    .prepareQuery(eventTimelineCF)
                                    .getKey(rowKey)
                                    .autoPaginate(true)
                                    .withColumnRange(new RangeBuilder().build());

                            while (!(columns = query.execute().getResult()).isEmpty()) {
                                MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
                                for (Column<String> c : columns) {
                                    // Deleting a row
                                    if (dryRun) {
                                        logger.info("Going to delete eventDetail for {} ", c.getStringValue());
                                    } else {
                                        m.withRow(aggEventResourcesStagingCF, c.getStringValue()).delete();
                                    }
                                }
                                if (!dryRun) {
                                    try {
                                        OperationResult<Void> result = m.execute();
                                    } catch (ConnectionException e) {
                                        java.util.logging.Logger.getLogger(CassandraDataLoader.class.getName()).log(Level.SEVERE, null, e);
                                    }
                                }
                            }
                        } catch (ConnectionException e) {
                        }


                    }
                }

                @Override
                public boolean failure(ConnectionException e) {
                    return true;  // Returning true will continue, false will terminate the query
                }
            });
        } catch (ConnectionException ex) {
            java.util.logging.Logger.getLogger(CassandraDataLoader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
