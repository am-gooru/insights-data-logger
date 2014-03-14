/*******************************************************************************
 * ActivityStreamDaoCassandraImpl.java
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class ActivityStreamDaoCassandraImpl extends BaseDAOCassandraImpl implements ActivityStreamDao{

    private static final Logger logger = LoggerFactory.getLogger(ActivityStreamDaoCassandraImpl.class);
    private final ColumnFamily<String, String> activityStreamCF;
    private static final String CF_ACTIVITY_STREAM = "activity_stream";
    
	public ActivityStreamDaoCassandraImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		activityStreamCF = new ColumnFamily<String, String>(
				CF_ACTIVITY_STREAM, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
	}

	 /**
     * @param activities
     *           raw data from event detail table
     * @throws ConnectionException
     *             if there is an error while inserting a record or unavailable host 
     */
	public void saveActivity(HashMap<String, Object> activities){
		 String rowKey = null;
		 String dateId = "0";
		 String columnName = null;
		 rowKey = activities.get("userUid").toString();
		 
		 if(activities.get("dateId") != null){
			 dateId = activities.get("dateId").toString();
		 } 
		 if(activities.get("existingColumnName") == null){
			 columnName = ((dateId.toString() == null ? "0L" : dateId.toString()) + "~" + (activities.get("eventName").toString() == null ? "NA" : activities.get("eventName").toString()) + "~" + activities.get("eventId").toString());
		 } else{
			 columnName = activities.get("existingColumnName").toString();
		 }
	 
	     try {
	        	
			 MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);	
			 m.withRow(activityStreamCF, rowKey)
			 .putColumnIfNotNull(columnName, activities.get("activity") != null ? activities.get("activity").toString():null, null)
			 .putColumnIfNotNull("username", activities.get("userName") != null ? activities.get("userName").toString():null, null)
			 .putColumnIfNotNull("user_uid", activities.get("userUid") != null ? activities.get("userUid").toString():null, null);
	            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra ", e);       
        }
	}
	
	public Map<String, Object> isEventIdExists(String userUid, String eventId){
		OperationResult<ColumnList<String>> eventColumns = null;
		Map<String,Object> resultMap = new HashMap<String, Object>();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		Boolean isExists = false;
		try {
			eventColumns = getKeyspace().prepareQuery(activityStreamCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			.getKey(userUid).execute();
			if (eventColumns != null)
				for(Column<String> eventColumn : eventColumns.getResult()){
					String columnName = eventColumn.getName();
					if (columnName != null) {						
					if(columnName.contains(eventId)){
						isExists = true;
						resultMap.put("isExists", isExists);
						resultMap.put("jsonString",eventColumn.getStringValue());
						resultMap.put("existingColumnName", columnName);
						resultList.add(resultMap);
						return resultMap;
					}
				}
			}
		} catch (ConnectionException e) {
			logger.info("Cassandra Connection Exception thrown!!");
			e.printStackTrace();
		}
		if(!isExists){
			resultMap.put("isExists", isExists);
			resultMap.put("jsonString",null);
			resultList.add(resultMap);
		}
		return resultMap;
	}
	
	public ColumnList<String> readColumnsWithPrefix(String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, String eventName, Integer rowsToRead){
		ColumnList<String> eventRows = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;
		Integer count = 30;
		
		if(eventName != null){
			startColumnPrefix = startColumnNamePrefix+"~"+eventName+"~\u0000";
			endColumnPrefix = endColumnNamePrefix+"~"+eventName+"~\uFFFF";
		} else {
			startColumnPrefix = startColumnNamePrefix+"~\u0000";
			endColumnPrefix = endColumnNamePrefix+"~\uFFFF";
		}
		if(rowsToRead != null){
			count = rowsToRead;
		}

		try {
			eventRows = getKeyspace().prepareQuery(activityStreamCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			.getKey(rowKey) .withColumnRange(//startColumnPrefix,endColumnPrefix, true, Integer.MAX_VALUE)
			new RangeBuilder().setLimit(count).setStart(startColumnPrefix).setEnd(endColumnPrefix).build())
			.execute().getResult();
			return eventRows;
		} catch (NotFoundException e) {
			e.printStackTrace();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return eventRows;
	}
	
    public void updateParentEvent(String key, String columnName, String columnValue ){
        
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        m.withRow(activityStreamCF, key)
                .putColumnIfNotNull(columnName, columnValue, null);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra  ");
        }
    
    	
    }

    public ColumnList<String> readLastNcolumns(String rowKey, Integer columnsToRead) {

    	ColumnList<String> activities = null;
    	try {
    		activities = getKeyspace().prepareQuery(activityStreamCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
    		.getKey(rowKey)
    		.withColumnRange(new RangeBuilder().setReversed().setLimit(columnsToRead.intValue()).build())
    		.execute().getResult();
    	} catch (ConnectionException e) {
    		e.printStackTrace();
    	}
    	return activities;
    }
    
}

