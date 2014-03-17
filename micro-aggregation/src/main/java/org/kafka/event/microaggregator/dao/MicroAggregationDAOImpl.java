/*******************************************************************************
 * MicroAggregationDAOImpl.java
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

import java.util.Map;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.DataUtils;
import org.kafka.event.microaggregator.core.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

public class MicroAggregationDAOImpl extends BaseDAOCassandraImpl implements  MicroAggregationDAO{

	 private static final Logger logger = LoggerFactory.getLogger(MicroAggregationDAOImpl.class);

	 private final ColumnFamily<String, String> microAggregationCF;
	 private static final String CF_EVENT_DETAILS_NAME = "micro_aggregation";
	 private final ColumnFamily<String, String> rtStudentReportCF;
	 private static final String CF_RT_STUDENT_REPORT = "rt_student_report";
	 private CassandraConnectionProvider connectionProvider;
	 private CollectionItemDAOImpl collectionItemDAOImpl;
	 
	public MicroAggregationDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
			microAggregationCF = new ColumnFamily<String, String>(
                CF_EVENT_DETAILS_NAME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
		   rtStudentReportCF = new ColumnFamily<String, String>(
	        		CF_RT_STUDENT_REPORT, // Column Family Name
	                StringSerializer.get(), // Key Serializer
	                StringSerializer.get()); // Column Serializer
		   
		   this.collectionItemDAOImpl = new CollectionItemDAOImpl(this.connectionProvider);
	}

	@Async
	public void microAggregation(Map<String,String> eventMap){
		
		long collectionViews = 0L;
		long resourceViews = 0L;
		long collectionReaction = 0L;
		long resourceReaction = 0L;
		long collectionAvgReaction = 0L;
		long resourceAvgReaction = 0L;
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			
			String keyOne = null;
			String keyTwo = null;
			
			if(eventMap.get("parentGooruId") != null && !eventMap.get("parentGooruId").equalsIgnoreCase("NA")){	
				String classPageId = collectionItemDAOImpl.getParentId(eventMap.get("parentGooruId"));	
				 keyOne = classPageId+"~"+eventMap.get("parentGooruId");
				 keyTwo = classPageId+"~"+eventMap.get("parentGooruId")+""+eventMap.get("contentGooruId");				
			}
					m.withRow(microAggregationCF, keyTwo)
					.putColumnIfNotNull(eventMap.get("contentGooruId") + "~" + eventMap.get("gooruUId"),+DataUtils.formatReactionString(eventMap.get("reactionType")))
					;
					
					m.withRow(microAggregationCF, keyOne)
					.putColumnIfNotNull(eventMap.get("contentGooruId") + "~" + eventMap.get("gooruUId"),+DataUtils.formatReactionString(eventMap.get("reactionType")))
					;
										
					
					try {
						m.execute();
					} catch (ConnectionException e) {
						logger.info("Error while inserting to cassandra  ");
					}
					
					collectionViews = this.getTotalViews(keyOne  ,eventMap.get("parentGooruId") + "~" +LoaderConstants.TOTALVIEWS.getName());
					collectionReaction = this.iterateColumnsAndSum(keyOne);
					collectionAvgReaction = Math.round(collectionReaction/collectionViews);
					updateAvgMetrics(keyOne,collectionReaction,eventMap.get("parentGooruId")+"~"+LoaderConstants.AVGRA.getName());
					
					resourceViews = getTotalViews(keyOne  ,eventMap.get("contentGooruId") + "~" +LoaderConstants.TOTALVIEWS.getName());
					resourceReaction = this.iterateColumnsAndSum(keyTwo);
					resourceAvgReaction = Math.round(resourceReaction/resourceViews);
					updateAvgMetrics(keyOne,collectionReaction,eventMap.get("contentGooruId")+"~"+LoaderConstants.AVGRA.getName());
	}

	private Long getTotalViews(String key,String ... columnName){

		long totalViews = 0L;
		ColumnList<String> stagedRecords = null;
    	try {
    		stagedRecords = getKeyspace().prepareQuery(rtStudentReportCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(key)
					 .withColumnSlice(columnName)
					 .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		for(String column : columnName){
			totalViews  = stagedRecords.getLongValue(column, 0L);
		}
		return totalViews;

	}

	private void updateAvgMetrics(String key,long metricValue,String metricName){
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		m.withRow(rtStudentReportCF, key)
		.putColumn(metricName,metricValue)
		;
		try {
			m.execute();
		} catch (ConnectionException e) {
			logger.info("Error while inserting to cassandra  ");
		}
	}
	
	public ColumnList<String> getRawStagedRecords(String Key){
		
		ColumnList<String> stagedRecords = null;
    	try {
    		stagedRecords = getKeyspace().prepareQuery(microAggregationCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(Key)
					 .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		return stagedRecords;
	}
	
	private boolean isRowAvailable(String key,String ... columnName){
		
		ColumnList<String> stagedRecords = null;
    	try {
    		stagedRecords = (getKeyspace().prepareQuery(microAggregationCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(key)
					 .withColumnSlice(columnName)
					 .execute().getResult());
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		
		return stagedRecords.isEmpty();
		
	}
	
	private Long iterateColumnsAndSum(String key){
		ColumnList<String> columns = null;
		Long values = 0L;
	
		try {
			columns = getKeyspace().prepareQuery(microAggregationCF)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getKey(key)
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}

		for(int i = 0 ; i < columns.size() ; i++) {
			values += columns.getColumnByIndex(i).getLongValue();
		}
		
		return values;
	}
}
