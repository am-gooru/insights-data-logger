/*******************************************************************************
 * RecentViewedResourcesDAOImpl.java
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

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class RealTImeAggregatorsDAOImpl extends BaseDAOCassandraImpl implements RealTImeAggregatorsDAO {


    private static final Logger logger = LoggerFactory.getLogger(RealTImeAggregatorsDAOImpl.class);
    private final ColumnFamily<String, String> realTimeAggregator;
    private static final String REAL_TIME_AGGREGATOR_CF = "real_time_aggregators";

	public RealTImeAggregatorsDAOImpl(
			CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		realTimeAggregator = new ColumnFamily<String, String>(
				   REAL_TIME_AGGREGATOR_CF, // Column Family Name
	                StringSerializer.get(), // Key Serializer
	                StringSerializer.get()); // Column Serializer
	}

	public void saveResource(String key , String gooruOid){
		
		//UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	    
		MutationBatch recentViewedResources = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    recentViewedResources.withRow(realTimeAggregator, key)
	    .putColumn("gooru_oid", gooruOid,null);
	
	    try {
	    	recentViewedResources.execute();
	    } catch (ConnectionException e) {
	        logger.info("Error while inserting event data to cassandra", e);
	    }
	

	}
	
	
	public ColumnList<String> readTimeLine(String timeLineKey) {


		ColumnList<String> dateDetail = null;
		try {
			dateDetail = getKeyspace().prepareQuery(realTimeAggregator)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getKey(timeLineKey)
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
    	return  dateDetail;
    }

	public Rows<String, String> readAll() {
		Rows<String, String> dataDetail = null;
		try {
			dataDetail = getKeyspace().prepareQuery(realTimeAggregator)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getAllRows()
					.withColumnRange(new RangeBuilder().setLimit(10).build()) 
					.execute()
					.getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
    	return  dataDetail;
    }
	
	public void deleteRow(String rowKey) {
        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(realTimeAggregator, rowKey).delete();
        try {
            OperationResult<Void> result = m.execute();

        } catch (ConnectionException ex) {
        	ex.printStackTrace();
        }
	}

	
}
