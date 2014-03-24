package org.logger.event.cassandra.loader.dao;

/*******************************************************************************
 * CounterDetailsDAOCassandraImpl.java
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.ednovo.data.model.EventData;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.TypeConverter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
public class CounterDetailsDAOCassandraImpl extends BaseDAOCassandraImpl implements CounterDetailsDAO,Constants {
	
    private static final Logger logger = LoggerFactory.getLogger(CounterDetailsDAOCassandraImpl.class);
    
    private final ColumnFamily<String, String> realTimeCounter;
    
    private final ColumnFamily<String, String> realTimeAggregator;
    
    private final ColumnFamily<String, String> microAggregator;
    
    private static final String CF_RT_COUNTER = "real_time_counter";
    
    private static final String CF_RT_AGGREGATOR = "real_time_aggregator";
    
    private static final String CF_MICRO_AGGREGATOR = "micro_aggregation";
    
    private CassandraConnectionProvider connectionProvider;
    
    private CollectionItemDAOImpl collectionItem;
    
    private EventDetailDAOCassandraImpl eventDetailDao; 
    
    private DimResourceDAOImpl dimResource;
    
    public CounterDetailsDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        realTimeCounter = new ColumnFamily<String, String>(
        		CF_RT_COUNTER, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
       
        realTimeAggregator = new ColumnFamily<String, String>(
        		CF_RT_AGGREGATOR, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
        microAggregator = new ColumnFamily<String, String>(
        		CF_MICRO_AGGREGATOR, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        this.collectionItem = new CollectionItemDAOImpl(this.connectionProvider);
        this.eventDetailDao = new EventDetailDAOCassandraImpl(this.connectionProvider);
        this.dimResource = new DimResourceDAOImpl(this.connectionProvider);
    }
    
    @Async
    public void realTimeMetrics(Map<String,String> eventMap,String aggregatorJson) throws JSONException{
    	
    	List<String> classPages = this.getClassPages(eventMap);
    	String key = eventMap.get(CONTENTGOORUOID);
		List<String> keysList = new ArrayList<String>();
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					keysList.add(ALLSESSION+classPage+SEPERATOR+key);
					keysList.add(ALLSESSION+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					this.clearAggregatoRow(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					this.clearCounterRow(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(RECENTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					if(!this.isRowAvailable(FIRSTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID))){
						keysList.add(FIRSTSESSION+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						this.addColumnForAggregator(FIRSTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
					
				}
			}
				keysList.add(ALLSESSION+key);
				keysList.add(ALLSESSION+key+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
				this.clearAggregatoRow(eventMap.get(SESSION)+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
				this.clearCounterRow(eventMap.get(SESSION)+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
				this.addColumnForAggregator(RECENTSESSION+key, eventMap.get(GOORUID), eventMap.get(SESSION));
				if(!this.isRowAvailable(FIRSTSESSION+key, eventMap.get(GOORUID))){
					keysList.add(FIRSTSESSION+key+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(FIRSTSESSION+key, eventMap.get(GOORUID), eventMap.get(SESSION));
				}
		}

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){

			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID));
					keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					this.clearAggregatoRow(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					this.clearCounterRow(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(RECENTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
					if(this.isRowAvailable(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID))){
						keysList.add(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						this.addColumnForAggregator(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
					}
				}
			}
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID));
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				this.clearAggregatoRow(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				this.clearCounterRow(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				this.addColumnForAggregator(RECENTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION));
				if(this.isRowAvailable(FIRSTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID))){
					keysList.add(FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(FIRSTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION));
				}
			
			
		}

		JSONObject j = new JSONObject(aggregatorJson);

		Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
    	
    	for (Entry entry : entrySet) {
        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
	        for(String localKey : keysList){
	        	if(e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(COUNTER)){
	        		if(!(entry.getKey().toString().equalsIgnoreCase(CHOICE)) &&!(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && eventMap.get(TYPE).equalsIgnoreCase(STOP)) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
	        			updateCounter(localKey,key+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()));
	        			updatePostAggregator(localKey,key+SEPERATOR+entry.getKey().toString());
	        			updateForPostAggregate(localKey+SEPERATOR+key, eventMap.get(GOORUID), 1L);
	        			
					}
	        		
	        		if(entry.getKey().toString().equalsIgnoreCase(CHOICE) && eventMap.get(RESOURCETYPE).equalsIgnoreCase(QUESTION)){
	    				int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get(ATTMPTTRYSEQ)) ;
	    				int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get(ATTMPTSTATUS)) ;
	    				String answerStatus = null;
	    				if(attempStatus.length == 0){
	    					answerStatus = LoaderConstants.SKIPPED.getName();
	        			}else {
	    	    			if(attempStatus[0] == 1){
	    	    				answerStatus = LoaderConstants.CORRECT.getName();
	    	    			}else if(attempStatus[0] == 0){
	    	    				answerStatus = LoaderConstants.INCORRECT.getName();
	    	    			}
	        			}	
	    				String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0]);
	    				updateCounter(localKey ,key+SEPERATOR+option,e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()));
	    				updateCounter(localKey ,key+SEPERATOR+answerStatus,1L);
	    				updatePostAggregator(localKey,key+SEPERATOR+option);
	    				updatePostAggregator(localKey,key+SEPERATOR+answerStatus);
					}
	        		
	        		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
		        		updateForPostAggregate(localKey,key+SEPERATOR+eventMap.get(GOORUID)+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
		        		updateForPostAggregate(localKey+SEPERATOR+key,eventMap.get(GOORUID)+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
	        		}
	        	}				
	        	if(e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(AGG)){

	        		if(e.get(AGGMODE)!= null &&  e.get(AGGMODE).toString().equalsIgnoreCase(AVG)){
	                   this.calculateAvg(localKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+e.get(DIVISOR).toString(), eventMap.get(CONTENTGOORUOID)+SEPERATOR+e.get(DIVIDEND).toString(), eventMap.get(CONTENTGOORUOID)+SEPERATOR+entry.getKey().toString());
	        		}
	        		
	        		if(e.get(AGGMODE)!= null && e.get(AGGMODE).toString().equalsIgnoreCase(SUMOFAVG)){
	                   long averageC = this.iterateAndFindAvg(localKey);
	                   this.updateRealTimeAggregator(localKey,eventMap.get(PARENTGOORUOID)+SEPERATOR+entry.getKey().toString(), averageC);
	                   long averageR = this.iterateAndFindAvg(localKey+SEPERATOR+eventMap.get(CONTENTGOORUOID));
	                   this.updateRealTimeAggregator(localKey,eventMap.get(CONTENTGOORUOID)+SEPERATOR+entry.getKey().toString(), averageR);
	               }
	        		if(e.get(AGGMODE)!= null && e.get(AGGMODE).toString().equalsIgnoreCase(SUM)){
		                   long sumOf = this.iterateAndFindSum(localKey+SEPERATOR+key);
		                   this.updateRealTimeAggregator(localKey,key+SEPERATOR+entry.getKey().toString(), sumOf);
		               }
	                        
	        	}
	        }
	        for(String localKey : keysList){
	        	this.realTimeStudentWiseReport(localKey,eventMap);
	        }
    	}
    }
    
    /**
     * @param key,columnName,count
     * @throws ConnectionException
     *             if host is unavailable
     */
    public void updateCounter(String key,String columnName, long count ) {

    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(realTimeCounter, key)
        .incrementCounterColumn(columnName, count);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }

    public void updateAggregator(String key,String columnName, long count ) {

    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(realTimeAggregator, key)
        .incrementCounterColumn(columnName, count);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }
    
    private void updatePostAggregator(String key,String columnName){
    	long value = this.getCounterLongValue(key, columnName);
    	this.updateAggregator(key, columnName,value);
    }
    
    public void updateForPostAggregate(String key,String columnName, long count ) {

    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(microAggregator, key)
        .putColumnIfNotNull(columnName, count);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra {} ", e);
        }
    }
    
    public void addColumnForAggregator(String key,String columnName, String  columnValue ) {

    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(microAggregator, key)
        .putColumnIfNotNull(columnName, columnValue);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra {} ", e);
        }
    }

    
    /**
     * @param key,metric
     * @return long value
     * 		return view count for resources
     * @throws ConnectionException
     *             if host is unavailable
     */
	public long getCounterLongValue(String key, String metric) {
		ColumnList<String>  result = null;
		Long count = 0L;
    	try {
    		 result = getKeyspace().prepareQuery(realTimeCounter).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
    	if (result.getLongValue(metric, null) != null) {
    		count = result.getLongValue(metric, null);
    	}
    	return (count);
	}

	public void realTimeStudentWiseReport(String keyValue,Map<String,String> eventMap) throws JSONException{

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		String resourceType = eventMap.get(RESOURCETYPE);
		String type = null;
		if(resourceType == null || resourceType.isEmpty()){
			type = COLLECTION;
		}else{
			type = RESOURCE;
		}
		m.withRow(realTimeAggregator, keyValue)
		.putColumnIfNotNull(type + "_gooru_oid",eventMap.get("contentGooruId"),null)
        ;

			if(resourceType != null && resourceType.equalsIgnoreCase(QUESTION)){		 
					if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){
						int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get(ATTMPTSTATUS)) ;
						int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get(ATTMPTTRYSEQ)) ;
						String openEndedText = eventMap.get(TEXT);						
						String answers = eventMap.get(ANS);
						JSONObject answersJson = new JSONObject(answers);
						JSONArray names = answersJson.names();
						String firstChoosenAns = null;
						if(names != null && names.length() != 0){
							firstChoosenAns = names.getString(0);
						}						
				      m.withRow(realTimeAggregator, keyValue)
				                .putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + "~"+TYPE ,eventMap.get(QUESTIONTYPE),null)
				      			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) +"~"+OPTIONS,DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0]),null)
				      			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + "~"+CHOICE,openEndedText,null)
				      			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + "~"+CHOICE,firstChoosenAns,null);
					}      				     
				}
			 try{
	         	m.execute();
	         } catch (ConnectionException e) {
	         	logger.info("Error while inserting to cassandra - JSON - ", e);
	         }
			
	}
			
	private void updateRealTimeAggregator(String key,String columnName,long columnValue){
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			m.withRow(realTimeAggregator, key)
			.putColumnIfNotNull(columnName,columnValue,null)
			;	
			try{
	         	m.execute();
	         } catch (ConnectionException e) {
	         	logger.info("Error while inserting to cassandra - JSON - ", e);
	         }
		}	

	private List<String> getClassPagesFromItems(List<String> parentIds){
		List<String> classPageGooruOids = new ArrayList<String>();
		for(String classPageGooruOid : parentIds){
			String type = dimResource.resourceType(classPageGooruOid);
			if(type != null && type.equalsIgnoreCase(LoaderConstants.CLASSPAGE.getName())){
				classPageGooruOids.add(classPageGooruOid);
			}
		}
		return classPageGooruOids;
	}
	
	public ColumnList<String> getRawStagedRecords(String Key){
		
		ColumnList<String> stagedRecords = null;
    	try {
    		stagedRecords = getKeyspace().prepareQuery(realTimeAggregator).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(Key)
					 .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		return stagedRecords;
	}	
	
	public Long getAggregatorLongValue(String key,String columnName){
		Column<String>  result = null;
		Long score = 0L;
    	try {
    		 result = getKeyspace().prepareQuery(realTimeAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .getColumn(columnName)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		
    	score = result.getLongValue();
    	return (score);
		
	}

	private boolean isRowAvailable(String key,String ... columnName){
		
		Rows<String, String> stagedRecords = null;
    	try {
    		stagedRecords = (getKeyspace().prepareQuery(realTimeAggregator).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKeySlice(key)
					 .withColumnSlice(columnName)
					 .execute().getResult());
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		
		return !stagedRecords.isEmpty();
		
	}
	
	private String getSession(String key,String columnName){
		
		String session = null; 
		
		Column<String> stagedRecords = null;
    	try {
    		stagedRecords = (getKeyspace().prepareQuery(realTimeAggregator).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(key)
					 .getColumn(columnName)
					 .execute().getResult());
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		
		if(stagedRecords != null){
			session = stagedRecords.getStringValue();
		}
		return session;
		
	}
	private List<String> getClassPages(Map<String,String> eventMap){
    	List<String> classPages = new ArrayList<String>();
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(PARENTGOORUOID) == null){
    		classPages = collectionItem.getParentId(eventMap.get(CONTENTGOORUOID));
    		classPages = this.getClassPagesFromItems(classPages);
    	}else if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
    		classPages.add(eventMap.get(PARENTGOORUOID));
    	}
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.get(PARENTGOORUOID) != null){
    		if(eventMap.get(CLASSPAGEGOORUOID) == null){
	    		ColumnList<String> eventDetail = eventDetailDao.readEventDetail(eventMap.get(PARENTEVENTID));
		    	if(eventDetail != null && eventDetail.size() > 0){
		    		if(eventDetail.getStringValue("event_name", null) != null && (eventDetail.getStringValue("event_name", null)).equalsIgnoreCase(LoaderConstants.CLPV1.getName())){
		    			classPages.add(eventDetail.getStringValue("content_gooru_oid", null));
		    		}		    		
		    		if(eventDetail.getStringValue("event_name", null) != null &&  (eventDetail.getStringValue("event_name", null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			    		if(eventDetail.getStringValue("parent_gooru_oid", null) == null || eventDetail.getStringValue("parent_gooru_oid", null).isEmpty()){
			    			classPages = collectionItem.getParentId(eventDetail.getStringValue("content_gooru_oid", null));
			    			classPages = this.getClassPagesFromItems(classPages);
			    		}else{
			    			classPages.add(eventDetail.getStringValue("parent_gooru_oid", null));
			    		}
		    		}
		    	}
	    	}else{
	    		classPages.add(eventMap.get(CLASSPAGEGOORUOID));
	    	}
    	}
	    	if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()) && eventMap.get(CLASSPAGEGOORUOID) == null)){
	    		if(eventMap.get(CLASSPAGEGOORUOID) == null){
	            ColumnList<String> R = eventDetailDao.readEventDetail(eventMap.get(PARENTEVENTID));
	            if(R != null && R.size() > 0){
	            	String parentEventId = R.getStringValue("parent_event_id", null);
	            	if(parentEventId != null ){
	            		ColumnList<String> C = eventDetailDao.readEventDetail(parentEventId);
	            		if(C.getStringValue("event_name", null) != null && (C.getStringValue("event_name", null)).equalsIgnoreCase(LoaderConstants.CLPV1.getName())){
			    			classPages.add(C.getStringValue("content_gooru_oid", null));
			    		}
			    		if(C.getStringValue("event_name", null) != null &&  (C.getStringValue("event_name", null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				    		if(C.getStringValue("parent_gooru_oid", null) == null || C.getStringValue("parent_gooru_oid", null).isEmpty()){
				    			classPages = collectionItem.getParentId(C.getStringValue("content_gooru_oid", null));
				    			classPages = this.getClassPagesFromItems(classPages);
				    		}else{
				    			classPages.add(C.getStringValue("parent_gooru_oid", null));
				    		}
			    		}
	            	}
	            }
	        }else{
	    		classPages.add(eventMap.get(CLASSPAGEGOORUOID));
	    	}
    	}
	    	return classPages;
	}
	
	private long iterateAndFindAvg(String key){
		ColumnList<String> columns = null;
		long values = 0L;
		long count = 0L; 
		long avgValues = 0L;
		try {
			columns = getKeyspace().prepareQuery(microAggregator)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getKey(key)
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		count = columns.size();
		if(columns != null && columns.size() > 0){
			for(int i = 0 ; i < columns.size() ; i++) {
				values += columns.getColumnByIndex(i).getLongValue();
			}
			avgValues = values/count;
		}
		
		return avgValues;
	}
	
	private long iterateAndFindSum(String key){
		Integer columns = null;
		try {
			columns = getKeyspace().prepareQuery(microAggregator)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getKey(key)
					.getCount()
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		
		return columns.longValue();
	}
	private void calculateAvg(String localKey,String divisor,String dividend,String columnToUpdate){
		long d = this.getCounterLongValue(localKey, divisor);
	    	if(d != 0L){
	    		long average = (this.getCounterLongValue(localKey, dividend)/d);
	    		this.updateRealTimeAggregator(localKey,columnToUpdate, average);
	    	}
    	}

	private void clearAggregatoRow(String key){
		
		MutationBatch m = getKeyspace().prepareMutationBatch();
		m.withRow(realTimeAggregator, key).delete();
		try {
			m.execute();
		} catch (ConnectionException e) {
			logger.info("Error while clearing counters : {}",e);
		}
	}
	
	private void clearCounterRow(String key){
		MutationBatch m = getKeyspace().prepareMutationBatch();
		m.withRow(realTimeCounter, key).delete();
		try {
			m.execute();
		} catch (ConnectionException e) {
			logger.info("Error while clearing counters : {}",e);
		}
	}
	@Override
	public void getIncrementer(EventData eventData) {
		// TODO Auto-generated method stub
		
	}

}
