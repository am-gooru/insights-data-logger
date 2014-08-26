/*******************************************************************************
 * LiveDashBoardDAOImpl.java
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

public class LiveDashBoardDAOImpl extends BaseDAOCassandraImpl implements  MicroAggregationDAO,Constants{

	private static final Logger logger = LoggerFactory.getLogger(LiveDashBoardDAOImpl.class);

    private final ColumnFamily<String, String> microAggregator;
    
    private static final String CF_MICRO_AGGREGATOR = "micro_aggregation";

    private final ColumnFamily<String, String> liveDashboard;
    
    private static final String CF_LIVE_DASHBOARD = "live_dashboard";

    private CassandraConnectionProvider connectionProvider;
    
    private CollectionItemDAOImpl collectionItem;
    
    private EventDetailDAOCassandraImpl eventDetailDao; 
    
    private DimResourceDAOImpl dimResource;
    
    private DimUserDAOCassandraImpl dimUser;
    
    private ClasspageDAOImpl classpage;
    
    private CollectionDAOImpl collection;
    
    private SimpleDateFormat customDateFormatter;

    private JobConfigSettingsDAOCassandraImpl configSettings;
    
    String dashboardKeys = null;
    
    public LiveDashBoardDAOImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        liveDashboard = new ColumnFamily<String, String>(
        		CF_LIVE_DASHBOARD, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
        microAggregator = new ColumnFamily<String, String>(
        		CF_MICRO_AGGREGATOR, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer

        this.collectionItem = new CollectionItemDAOImpl(this.connectionProvider);
        this.eventDetailDao = new EventDetailDAOCassandraImpl(this.connectionProvider);
        this.dimResource = new DimResourceDAOImpl(this.connectionProvider);
        this.classpage = new ClasspageDAOImpl(this.connectionProvider);
        this.collection = new CollectionDAOImpl(this.connectionProvider);
        this.dimUser = new DimUserDAOCassandraImpl(this.connectionProvider);
        this.configSettings = new JobConfigSettingsDAOCassandraImpl(this.connectionProvider);    
        this.customDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
        dashboardKeys = configSettings.getConstants("dashboard~keys","constant_value");

    }

    public void findDifferenceInCount(Map<String,String> eventMap) throws ParseException{
    	
    	Map<String,String>  aggregator = this.generateKeyValues(eventMap);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	
    	for (Map.Entry<String, String> entry : aggregator.entrySet()) {
    		
    	    long thisCount = this.getLiveLongValue(entry.getKey(), COUNT+SEPERATOR+eventMap.get(EVENTNAME));
    	    long lastCount = this.getLiveLongValue(entry.getValue(), COUNT+SEPERATOR+eventMap.get(EVENTNAME));
    	    if(lastCount != 0L){
    	    	long difference = (thisCount*100)/lastCount;
    	    	this.generateAggregator(thisCount+SEPERATOR+lastCount, DIFF+SEPERATOR+eventMap.get(EVENTNAME), String.valueOf(difference), m);
    	    }
    	}    	
    	try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }    
    
    public void generateCounter(String key,String columnName, long count ,MutationBatch m) {
        m.withRow(liveDashboard, key)
        .incrementCounterColumn(columnName, count);
    }
    
    public void generateAggregator(String key,String columnName, String value ,MutationBatch m) {
        m.withRow(microAggregator, key)
        .putColumnIfNotNull(columnName, value);
    }
    
	public void addRowColumn(String rowKey,String columnName,String value){

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		
		m.withRow(microAggregator, rowKey)
		.putColumnIfNotNull(columnName, value)
		;
		 try{
	         	m.execute();
	         } catch (ConnectionException e) {
	         	logger.info("Error while adding session - ", e);
	         }
	}
	
	private boolean isRowAvailable(String key,String  columnName){
		ColumnList<String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(microAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		if (result != null && !result.isEmpty() && result.getColumnByName(columnName) != null) {
				return true;
    	}		
		return false;
		
	}
	
	private Long getLiveLongValue(String key,String  columnName){

		Column<String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(liveDashboard)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .getColumn(columnName)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		return result.getLongValue();
		
	}
	
	private String getLiveStringValue(String key,String  columnName){
		Column<String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(liveDashboard)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .getColumn(columnName)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		return result.getStringValue();
		
	}
	
	public Map<String,String> generateKeyValues(Map<String,String> eventMap) throws ParseException{
		Map<String,String> returnDate = new LinkedHashMap<String, String>();
		if(dashboardKeys != null){
			for(String key : dashboardKeys.split(",")){
				String rowKey = null;
				if(!key.equalsIgnoreCase("all")) {
					customDateFormatter = new SimpleDateFormat(key);
					Date eventDateTime = new Date(Long.valueOf(eventMap.get(STARTTIME)));
					rowKey = customDateFormatter.format(eventDateTime);
					Date lastDate = customDateFormatter.parse(rowKey);
					Date rowValues = new Date(lastDate.getTime() - 2);
					returnDate.put(customDateFormatter.format(lastDate), customDateFormatter.format(rowValues));
					if(eventMap.get(ORGANIZATIONUID) != null && !eventMap.get(ORGANIZATIONUID).isEmpty()){
						returnDate.put(customDateFormatter.format(lastDate)+SEPERATOR+eventMap.get(ORGANIZATIONUID), customDateFormatter.format(rowValues)+SEPERATOR+eventMap.get(ORGANIZATIONUID));
					}
					if(eventMap.get(GOORUID) != null && !eventMap.get(GOORUID).isEmpty()){
						returnDate.put(customDateFormatter.format(lastDate)+SEPERATOR+eventMap.get(GOORUID), customDateFormatter.format(rowValues)+SEPERATOR+eventMap.get(GOORUID));
					}
				} 
			}
		}
		return returnDate; 
	}
}
