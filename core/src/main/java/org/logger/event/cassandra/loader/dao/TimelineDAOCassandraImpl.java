/*******************************************************************************
 * TimelineDAOCassandraImpl.java
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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.logger.event.cassandra.loader.dao;

import java.util.UUID;

import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class TimelineDAOCassandraImpl extends BaseDAOCassandraImpl implements TimelineDAO {

    private ColumnFamily<String, String> eventTimelineCF;
    private ColumnFamily<String, String> sessionTimelineCF;
    private static final String CF_EVENT_NAME = "event_timeline";
    private static final Logger logger = LoggerFactory.getLogger(TimelineDAOCassandraImpl.class);

    /**
     *
     */
    public TimelineDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        eventTimelineCF = new ColumnFamily<String, String>(CF_EVENT_NAME,
                StringSerializer.get(),
                StringSerializer.get());
    }

    public void updateTimeline(EventData eventData, String rowKey) {

        UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        MutationBatch eventTimelineMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        eventTimelineMutation.withRow(eventTimelineCF, rowKey).putColumn(
                eventColumnTimeUUID.toString(), eventData.getEventKeyUUID(), null);

        eventTimelineMutation.withRow(eventTimelineCF, (rowKey+"~"+eventData.getEventName())).putColumn(
                eventColumnTimeUUID.toString(), eventData.getEventKeyUUID(), null);
        try {
            eventTimelineMutation.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting event data to cassandra", e);
        }
    }

    public void updateTimelineObject(EventObject eventObject, String rowKey,String CoulmnValue) {

        UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        MutationBatch eventTimelineMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        eventTimelineMutation.withRow(eventTimelineCF, rowKey).putColumn(
                eventColumnTimeUUID.toString(), CoulmnValue, null);

        eventTimelineMutation.withRow(eventTimelineCF, (rowKey+"~"+eventObject.getEventName())).putColumn(
                eventColumnTimeUUID.toString(), CoulmnValue, null);
        try {
            eventTimelineMutation.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting event data to cassandra", e);
        }
    }
    
	public ColumnList<String> readTimeLine(String timeLineKey) {


		ColumnList<String> dateDetail = null;
		try {
			dateDetail = getKeyspace().prepareQuery(eventTimelineCF)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getKey(timeLineKey)
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
    	return  dateDetail;
    }

	public void delete(String rowKey) {
        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(eventTimelineCF, rowKey).delete();
        try {
            OperationResult<Void> result = m.execute();

        } catch (ConnectionException ex) {
        	logger.info("Error while deleting event data from cassandra", ex);
        	return;
        }
	}
}
