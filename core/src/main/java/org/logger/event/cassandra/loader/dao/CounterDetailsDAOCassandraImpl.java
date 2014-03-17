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
package org.logger.event.cassandra.loader.dao;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
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

public class CounterDetailsDAOCassandraImpl extends BaseDAOCassandraImpl implements CounterDetailsDAO {
	
    private static final Logger logger = LoggerFactory.getLogger(CounterDetailsDAOCassandraImpl.class);
    private final ColumnFamily<String, String> counterDetailsCF;
    private final ColumnFamily<String, String> rtStudentReportCF;
    private static final String CF_COUNTER_DETAILS_NAME = "real_time_counters";
    private static final String CF_RT_STUDENT_REPORT = "rt_student_report";
    private RecentViewedResourcesDAOImpl recentViewedResources;
    private CassandraConnectionProvider connectionProvider;
    private CollectionItemDAOImpl collectionItemDAOImpl;
    
    public CounterDetailsDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        counterDetailsCF = new ColumnFamily<String, String>(
                CF_COUNTER_DETAILS_NAME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
       
        rtStudentReportCF = new ColumnFamily<String, String>(
        		CF_RT_STUDENT_REPORT, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        this.recentViewedResources = new RecentViewedResourcesDAOImpl(this.connectionProvider);
        this.collectionItemDAOImpl = new CollectionItemDAOImpl(this.connectionProvider);
    }
    
    @Override
    public void getIncrementer(EventData eventData) {
    	if(
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CRPD.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CQRPD.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CPD.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CP.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CRP.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.QUIZP.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.QUIZPRV.getName())) || 
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.RPD.getName())) ||
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.QOPD.getName())) ||
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.CROPD.getName())) ||
    			(eventData.getEventName().equalsIgnoreCase(LoaderConstants.QPD.getName()))  && 
    			(eventData.getType().equalsIgnoreCase("start")) )  {
    		String gooruOid = eventData.getContentGooruId();
    		if(gooruOid == null){
    			gooruOid = eventData.getGooruOId();
    		}
    		if(gooruOid == null){
    			gooruOid = eventData.getGooruId();
    		}    		
    		if (gooruOid != null) {
    			SimpleDateFormat dateFormats = new SimpleDateFormat("yyyyMMddkkmmss");
    			Date date = new Date();
    			this.updateCounter(gooruOid,LoaderConstants.VIEWS.getName(),1);
    			recentViewedResources.saveResource(gooruOid, gooruOid);
    		}
    		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    		Date date = new Date();
    		
    		this.updateCounter(dateFormatter.format(date),eventData.getEventName(),1);
    	}
    }

    @Async
    public void realTimeMetrics(Map<String,String> eventMap,String aggregatorJson) throws JSONException{
    	JSONObject j = new JSONObject(aggregatorJson);
    	Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
    	for (Entry entry : entrySet) {
        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
        		if(e.get("aggregatorType").toString().equalsIgnoreCase("counter")){
        			String key = eventMap.get("contentGooruId");
        			//For Custom Student Real Time Report
        			if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CPV1.getName())){
        				if(!entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && !eventMap.get("type").equalsIgnoreCase("stop")){
	        				String localKey = null;
        					if(eventMap.get("parentGooruId") == null || eventMap.get("parentGooruId").equalsIgnoreCase("NA")){
	        					String parentGooruOid = collectionItemDAOImpl.getParentId(key);
	        					localKey = parentGooruOid+"~"+key;
	        				}else{
	        					localKey = eventMap.get("parentGooruId")+key; 
	        				}
	            			updateCounter(localKey,key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : Long.parseLong(eventMap.get(e.get("aggregatorMode")).toString()));
	            			updateCounter(localKey+ "~" + eventMap.get("gooruUId"),key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : Long.parseLong(eventMap.get(e.get("aggregatorMode")).toString()));
        				}
        			}
        			
        			if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
        			if(!entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && !eventMap.get("type").equalsIgnoreCase("stop")){
        				String classPageOid = collectionItemDAOImpl.getParentId(eventMap.get("parentGooruId"));
        				String localKey = classPageOid+"~"+eventMap.get("parentGooruId")+"~"+key;
        				updateCounter(localKey,key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : Long.parseLong(eventMap.get(e.get("aggregatorMode")).toString()));
            			updateCounter(localKey+ "~" + eventMap.get("gooruUId"),key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : Long.parseLong(eventMap.get(e.get("aggregatorMode")).toString()));
            			if(entry.getKey().toString().equalsIgnoreCase("choice") && eventMap.get("resourceType").equalsIgnoreCase("question")){
            				int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get("attemptTrySequence")) ;
            				String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0]);
            				updateCounter(localKey ,key+"~"+entry.getKey().toString()+"~"+option,e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : Long.parseLong(eventMap.get(e.get("aggregatorMode")).toString()));
            				}
        				}
        			}
        			if(eventMap.get("eventName").equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
        				String classPageOid = collectionItemDAOImpl.getParentId(eventMap.get("parentGooruId"));
        				String localKey = classPageOid+"~"+eventMap.get("parentGooruId");
        				updateCounter(localKey,key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : DataUtils.formatReactionString(eventMap.get(e.get("aggregatorMode")).toString()));
            			updateCounter(localKey+ "~" + eventMap.get("gooruUId"),key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : DataUtils.formatReactionString(eventMap.get(e.get("aggregatorMode")).toString()));
            			updateCounter(localKey,key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : DataUtils.formatReactionString(eventMap.get(e.get("aggregatorMode")).toString()));
            			updateCounter(localKey+"~"+ eventMap.get("gooruUId"),key+"~"+entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : DataUtils.formatReactionString(eventMap.get(e.get("aggregatorMode")).toString()));
        			}
        			//Resource view count
        			updateCounter(key,entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : DataUtils.formatReactionString(eventMap.get(e.get("aggregatorMode")).toString()));
        			//Resource view count based on user
        			updateCounter(key+"~"+ eventMap.get("gooruUId"),entry.getKey().toString(),e.get("aggregatorMode").toString().equalsIgnoreCase("auto") ? 1L : DataUtils.formatReactionString(eventMap.get(e.get("aggregatorMode")).toString()));
        		}
        }
    }
    
    /**
     * @param key,columnName,count
     * @throws ConnectionException
     *             if host is unavailable
     */
    public void updateCounter(String key,String columnName, long count ) {

    	MutationBatch m = getKeyspace().prepareMutationBatch();
        m.withRow(counterDetailsCF, key)
        .incrementCounterColumn(columnName, count);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }
    

    /**
     * @param key,metric
     * @return long value
     * 		return view count for resources
     * @throws ConnectionException
     *             if host is unavailable
     */
	public long readViewCount(String key, String metric) {
		ColumnList<String>  result = null;
		Long count = 0L;
    	try {
    		 result = getKeyspace().prepareQuery(counterDetailsCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
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

	public void realTimeStudentWiseReport(Map<String,String> eventMap) throws JSONException{

		String resourceType = eventMap.get("resourceType");
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		  
		HashMap<String, String>  keys= keyGeneration(eventMap);
			if(keys != null){
			for (String keyValue : keys.values()) {	
					m.withRow(rtStudentReportCF, keyValue)
					.putColumnIfNotNull(resourceType + "_gooru_oid",eventMap.get("contentGooruId"),null)
					;					
			if(resourceType != null && resourceType.equalsIgnoreCase("question")){		 
					Long studentCurrentScore = 0L;
					if(eventMap.get("type").equalsIgnoreCase("stop") && !isRowAvailable(keyValue, eventMap.get("contentGooruId")+"~choice")){
						int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get("attemptStatus")) ;
						int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get("attemptTrySequence")) ;
						String openEndedText = eventMap.get("text");						
						String answers = eventMap.get("answers");
						JSONObject answersJson = new JSONObject(answers);
						JSONArray names = answersJson.names();
						String firstChoosenAns = null;
						if(names != null && names.length() != 0){
							firstChoosenAns = names.getString(0);
						}						
					 if(attempStatus.length !=0 && attempStatus[0] != 0){
						   studentCurrentScore = (getRTLongValues(keyValue,LoaderConstants.SCORE.getName()) + attempStatus[0]);
					  }
					  if(eventMap.get("score") != null && Integer.parseInt(eventMap.get("score")) != 0){
						  studentCurrentScore = Long.parseLong(eventMap.get("score")); 
					  }
				      m.withRow(rtStudentReportCF, keyValue)
				                //.putColumnIfNotNull(eventMap.get("contentGooruId") +"~score", studentCurrentScore, null)
				                .putColumnIfNotNull(eventMap.get("contentGooruId") + "~Type" ,eventMap.get("questionType"),null)
				      			.putColumnIfNotNull(eventMap.get("contentGooruId") +"~choice",DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0]),null)
				      			.putColumnIfNotNull(eventMap.get("contentGooruId") + "~choice",openEndedText,null)
				      			.putColumnIfNotNull(eventMap.get("contentGooruId") + "~choice",firstChoosenAns,null)
				      			//.putColumnIfNotNull(eventMap.get("contentGooruId") +"~"+DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0]),(getRTLongValues(keyValue,eventMap.get("contentGooruId")+"~"+DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0])) + 1),null)
				      			.putColumnIfNotNull(eventMap.get("contentGooruId") +"~"+DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[0]) +"~status",attempStatus[0],null);
					}      				     
				}
			 try{
	         	m.execute();
	         } catch (ConnectionException e) {
	         	logger.info("Error while inserting to cassandra - JSON - ", e);
	         }
			}
		}
}
			

	private Map<String,Long> aggregatingMetrics(String key,Map<String,String> eventMap){

		Map<String,Long> aggregatedRecords = new HashMap<String, Long>();
		long totalViews = 1L;
		long totalTimeSpent = Long.parseLong(eventMap.get("totalTimeSpentInMs"));
		long avgTimeSpent =  0L;
		ColumnList<String> stagedRecords = getRawStagedRecords(key);
		
		if(eventMap.get("type").equalsIgnoreCase("start")){
			totalViews = (totalViews+stagedRecords.getLongValue(eventMap.get("contentGooruId")+"~"+LoaderConstants.TOTALVIEWS.getName(), 1L));
		}else{
			totalViews = stagedRecords.getLongValue(eventMap.get("contentGooruId")+"~"+LoaderConstants.TOTALVIEWS.getName(), 1L);
		}
		
		totalTimeSpent = (totalTimeSpent+stagedRecords.getLongValue(eventMap.get("contentGooruId")+"~"+LoaderConstants.TS.getName(), 0L));
		avgTimeSpent = (totalTimeSpent/totalViews);
		aggregatedRecords.put(eventMap.get("contentGooruId")+"~"+LoaderConstants.TS.getName(), totalTimeSpent);
		aggregatedRecords.put(eventMap.get("contentGooruId")+"~"+LoaderConstants.TOTALVIEWS.getName(), totalViews);
		aggregatedRecords.put(eventMap.get("contentGooruId")+"~"+LoaderConstants.AVGTS.getName(), avgTimeSpent);
		
		return aggregatedRecords;
	}

	private HashMap<String, String> keyGeneration(Map<String,String> eventMap){
		
		HashMap<String, String> keys = new HashMap<String, String>();
		String keyOne = null;
		String keyTwo = null ;
				String parentGooruOid = eventMap.get("parentGooruId");
				parentGooruOid = collectionItemDAOImpl.getParentId(eventMap.get("parentGooruId"));
				keyOne = parentGooruOid+"~"+eventMap.get("parentGooruId");
				keyTwo = parentGooruOid+"~"+eventMap.get("parentGooruId") + "~" + eventMap.get("gooruUId");			
				keys.put("keyOne", keyOne);
				keys.put("keyTwo", keyTwo);
				return keys;
			
	}
	public ColumnList<String> getRawStagedRecords(String Key){
		
		ColumnList<String> stagedRecords = null;
    	try {
    		stagedRecords = getKeyspace().prepareQuery(rtStudentReportCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(Key)
					 .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		return stagedRecords;
	}	
	public Long getRTLongValues(String key,String columnName){
		
		Column<String>  result = null;
		Long score = 0L;
    	try {
    		 result = getKeyspace().prepareQuery(rtStudentReportCF)
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
    		stagedRecords = (getKeyspace().prepareQuery(rtStudentReportCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKeySlice(key)
					 .withColumnSlice(columnName)
					 .execute().getResult());
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		
		logger.info("RT Record is empty : {}",stagedRecords.isEmpty());
		return stagedRecords.isEmpty();
		
	}


/*	public static void main(String a[]) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String,String> map = new HashMap<String,String>();
		try {
			 
			//convert JSON string to Map
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue("{\"gooruUid\":\"ANONYMOUS\"}", 
			    new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue("{\"eventName\":\"collection-resource-play-dots\"}", 
				    new TypeReference<HashMap<String,String>>(){}));
			
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue("{\"contentGooruOid\":\"41e8f85a-2a3f-4dc6-ad1b-f4a9c4903e17\",\"parentGooruOid\":\"c5256218-5c09-4fb9-84eb-e9bc7a5fe043\",\"type\":\"start\"}", 
				    new TypeReference<HashMap<String,String>>(){}));
			
			System.out.println(map.get("parentGooruOid"));
	 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
}
