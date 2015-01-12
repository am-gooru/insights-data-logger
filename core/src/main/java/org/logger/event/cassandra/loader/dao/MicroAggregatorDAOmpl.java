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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.TypeConverter;
import org.ednovo.data.model.UserCo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class MicroAggregatorDAOmpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO,Constants {
	
    private static final Logger logger = LoggerFactory.getLogger(MicroAggregatorDAOmpl.class);
    
    private CassandraConnectionProvider connectionProvider;
    
    private SimpleDateFormat secondsDateFormatter;
    
    private long questionCountInQuiz = 0L;
    
    private BaseCassandraRepoImpl baseCassandraDao;
    
	private RawDataUpdateDAOImpl rawUpdateDAO;
    
    public static  Map<String,Object> cache;
        
    public MicroAggregatorDAOmpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        this.baseCassandraDao = new BaseCassandraRepoImpl(this.connectionProvider);
        this.secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
        cache = new LinkedHashMap<String, Object>();
        this.rawUpdateDAO = new RawDataUpdateDAOImpl(this.connectionProvider);
    }
    
    @Async
    public void callCounters(Map<String,String> eventMap) throws JSONException, ParseException {
    	String contentGooruOId = "";
		String eventName = "";
		String gooruUId = "";
		long start = System.currentTimeMillis();
		if(eventMap.containsKey(EVENTNAME) && eventMap.containsKey(GOORUID)) {
			contentGooruOId = eventMap.get(CONTENTGOORUOID);
			gooruUId = eventMap.get(GOORUID);
			eventName = eventMap.get(EVENTNAME);
			Calendar currentDate = Calendar.getInstance(); //Get the current date			
			int week = currentDate.get(Calendar.WEEK_OF_MONTH);
			int month = currentDate.get(Calendar.MONTH);
			month = month + 1;
			int year = currentDate.get(Calendar.YEAR);
			int date = currentDate.get(Calendar.DATE);

			List<String> returnDate = new ArrayList<String>();
			returnDate.add(year+SEPERATOR+month+SEPERATOR+date+SEPERATOR+eventName);
			returnDate.add(year+SEPERATOR+month+SEPERATOR+week+SEPERATOR+eventName);
			returnDate.add(year+SEPERATOR+month+SEPERATOR+eventName);
			returnDate.add(year+SEPERATOR+eventName);
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for(String key : returnDate) {
				baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), key,contentGooruOId+SEPERATOR+VIEWS,1, m);
				baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),key,gooruUId+SEPERATOR+VIEWS,1, m);
				baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),key,VIEWS,1,m);
			}
			try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
	        }
		}
		
		long stop = System.currentTimeMillis();
		logger.info("Time spent for counters : {}",(stop-start));	
    }
    @Async
    public void realTimeMetrics(Map<String, String> eventMap,String aggregatorJson) throws Exception{
    	if(cache.size() > 100000){
    		cache = new LinkedHashMap<String, Object>();
    	}
    	List<String> classPages = this.getClassPages(eventMap);
    	String key = eventMap.get(CONTENTGOORUOID);
		List<String> keysList = new ArrayList<String>();
		
		boolean isStudent = false;
		/*
		 * 
		 * Update last accessed time/user
		 * 
		 */
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){			
			baseCassandraDao.saveLongValue(ColumnFamily.RESOURCE.getColumnFamily(),eventMap.get(CONTENTGOORUOID), "lastAccessed", Long.parseLong(eventMap.get("endTime")));
			baseCassandraDao.saveLongValue(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+eventMap.get(CONTENTGOORUOID), "last_accessed", Long.parseLong(eventMap.get("endTime")));
			
		}else if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
			baseCassandraDao.saveLongValue(ColumnFamily.RESOURCE.getColumnFamily(),eventMap.get(CONTENTGOORUOID), "lastAccessed", Long.parseLong(eventMap.get("endTime")));
			baseCassandraDao.saveLongValue(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+eventMap.get(CONTENTGOORUOID), "last_accessed", Long.parseLong(eventMap.get("endTime")));
			//baseCassandraDao.saveLongValue(ColumnFamily.RESOURCE.getColumnFamily(),eventMap.get(PARENTGOORUOID), "lastAccessed", Long.parseLong(eventMap.get("endTime")));
			//baseCassandraDao.saveLongValue(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+eventMap.get(PARENTGOORUOID), "last_accessed", Long.parseLong(eventMap.get("endTime")));
		}else{
			baseCassandraDao.saveLongValue(ColumnFamily.RESOURCE.getColumnFamily(),eventMap.get(CONTENTGOORUOID), "lastAccessed", Long.parseLong(eventMap.get("endTime")));
			baseCassandraDao.saveLongValue(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+eventMap.get(CONTENTGOORUOID), "last_accessed", Long.parseLong(eventMap.get("endTime")));
		}
		baseCassandraDao.saveStringValue(ColumnFamily.RESOURCE.getColumnFamily(),eventMap.get(CONTENTGOORUOID), "lastAccessedUser", eventMap.get(GOORUID));
		baseCassandraDao.saveStringValue(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+eventMap.get(CONTENTGOORUOID), "last_accessed_user", eventMap.get(GOORUID));
		
		/*Maintain session - Start */
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
	        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
	        
	        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
	        	/*isStudent = classpage.isUserPartOfClass(eventMap.get(GOORUID),eventMap.get(PARENTGOORUOID));
	        	if(isStudent){*/
	        	baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
	        		
	        	//}
	        }
			baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
		}
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
			Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
	        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
	        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
	        	if(classPages != null && classPages.size() > 0){
					for(String classPage : classPages){
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
					}
	        	}
	        }
			baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
		}
		
		/*Maintain session - END */
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			questionCountInQuiz = this.getQuestionCount(eventMap);
			if(classPages != null && classPages.size() > 0){
				for(String classPage : classPages){
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
					
					logger.info("isOwner : {}",isOwner);
					
					if(cache.containsKey(eventMap.get(GOORUID)+SEPERATOR+classPage)){
						isStudent = (Boolean) cache.get(eventMap.get(GOORUID)+SEPERATOR+classPage);
					}else{
						
					isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
		
					int retryCount = 1;
			        while (retryCount < 5 && !isStudent) {
			        	Thread.sleep(500);
			        	isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
			        	logger.info("retrying to check if a student : {}",retryCount);
			            retryCount++;
			        }
			        	cache.put(eventMap.get(GOORUID)+SEPERATOR+classPage,isStudent);
					}
					logger.info("isStudent : {}",isStudent);
					
					eventMap.put(CLASSPAGEGOORUOID, classPage);
					if(!isOwner && isStudent){
					keysList.add(ALLSESSION+classPage+SEPERATOR+key);
					keysList.add(ALLSESSION+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					}
					
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					logger.info("Recent Key 1: {} ",eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					
					if(!this.isRowAvailable(FIRSTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
						keysList.add(FIRSTSESSION+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
					
				}
			}
				keysList.add(ALLSESSION+eventMap.get(CONTENTGOORUOID));
				keysList.add(ALLSESSION+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				logger.info("Recent Key 2: {} ",eventMap.get(SESSION)+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+eventMap.get(CONTENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
				if(!this.isRowAvailable(FIRSTSESSION+eventMap.get(CONTENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION))){
					keysList.add(FIRSTSESSION+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+eventMap.get(CONTENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
				}
		}

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){

			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
					
					if(cache.containsKey(eventMap.get(GOORUID)+SEPERATOR+classPage)){
						isStudent = (Boolean) cache.get(eventMap.get(GOORUID)+SEPERATOR+classPage);
					}else{
						
					isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);

					int retryCount = 1;
			        while (retryCount < 5 && !isStudent) {
			        	Thread.sleep(500);
			        	isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
			        	logger.info("retrying to check if a student : {}",retryCount);
			            retryCount++;
			        }
			        cache.put(eventMap.get(GOORUID)+SEPERATOR+classPage,isStudent);
					}
					logger.info("isStudent : {}",isStudent);
					
					if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID));
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					}
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
					if(!this.isRowAvailable(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
						keysList.add(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
				}
			}
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID));
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION));
				if(!this.isRowAvailable(FIRSTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION))){
					keysList.add(FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION));
				}
			
			
		}

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				}
			}
		}
		if(keysList != null && keysList.size() > 0 ){
			
			this.startCounters(eventMap, aggregatorJson, keysList, key);
			this.postAggregatorUpdate(eventMap, aggregatorJson, keysList, key);
			this.startCounterAggregator(eventMap, aggregatorJson, keysList, key);
		}
     }
    
    public void postAggregatorUpdate(Map<String,String> eventMap,String aggregatorJson,List<String> keysList,String key) throws JSONException{
    	JSONObject j = new JSONObject(aggregatorJson);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
    	
    	for (Entry entry : entrySet) {
        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
	        for(String localKey : keysList){
	        	if(e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(COUNTER)){
	        		if(!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE)) &&!(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && eventMap.get(TYPE).equalsIgnoreCase(STOP)) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
	        		//if(!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE)) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
	        			long value = this.getCounterLongValue(localKey, key+SEPERATOR+entry.getKey().toString());
	        			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey, key+SEPERATOR+entry.getKey().toString(),value,m);
	        	    	
					}
	        		
	        		if(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE) && eventMap.get(RESOURCETYPE).equalsIgnoreCase(QUESTION) && eventMap.get(TYPE).equalsIgnoreCase(STOP)){
	    				int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get(ATTMPTTRYSEQ)) ;
	    				int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get(ATTMPTSTATUS)) ;
	    				String answerStatus = null;
	    				int status = 0;
	    					status = Integer.parseInt(eventMap.get("attemptCount"));
	    					if(status != 0){
						         status = status-1;
	    					}
						if(attempStatus[status] == 1){
							answerStatus = LoaderConstants.CORRECT.getName();
						}else if(attempStatus[status] == 0){
							answerStatus = LoaderConstants.INCORRECT.getName();
						}
	    	    		String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[status]);
	    	    		if(option != null && option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){
	    	    			answerStatus = 	option;
	    	    		}
	    				String openEndedText = eventMap.get(TEXT);
	    				if(eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE) && openEndedText != null && !openEndedText.isEmpty()){
	    					option = "A";
	    				}
	    				boolean answered = this.isUserAlreadyAnswered(localKey, key);
        				
	    				if(answered){
	    					if(!option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){
	    						long value = this.getCounterLongValue(localKey,key+SEPERATOR+option);
	    						baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,key+SEPERATOR+option,value,m);	    						
	    					}
	    				}else{
	    					long value = this.getCounterLongValue(localKey,key+SEPERATOR+option);
    						baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,key+SEPERATOR+option,value,m);	    
	    				}
	    				if(!eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE) && !answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){	    					
	    					long values = this.getCounterLongValue(localKey,key+SEPERATOR+answerStatus);
		        	    	baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,key+SEPERATOR+answerStatus,values,m);
	    				}
					}
	        		
	        	}	
	        	this.realTimeAggregator(localKey,eventMap);
	        }
    	
    	}
    	try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }

    public void startCounterAggregator(Map<String,String> eventMap,String aggregatorJson,List<String> keysList,String key) throws JSONException{
    	
    	JSONObject j = new JSONObject(aggregatorJson);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
    	
    	for (Entry entry : entrySet) {
        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
	        for(String localKey : keysList){
	        	if(e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(AGG)){
	        		if(e.get(AGGMODE)!= null &&  e.get(AGGMODE).toString().equalsIgnoreCase(AVG)){
	                   this.calculateAvg(localKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+e.get(DIVISOR).toString(), eventMap.get(CONTENTGOORUOID)+SEPERATOR+e.get(DIVIDEND).toString(), eventMap.get(CONTENTGOORUOID)+SEPERATOR+entry.getKey().toString());
	        		}
	        		
	        		if(e.get(AGGMODE)!= null && e.get(AGGMODE).toString().equalsIgnoreCase(SUMOFAVG)){
	                   long averageC = this.iterateAndFindAvg(localKey);
	                   baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,eventMap.get(PARENTGOORUOID)+SEPERATOR+entry.getKey().toString(), averageC);
	                   long averageR = this.iterateAndFindAvg(localKey+SEPERATOR+eventMap.get(CONTENTGOORUOID));
	                   baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,eventMap.get(CONTENTGOORUOID)+SEPERATOR+entry.getKey().toString(), averageR);
	               }
	        		if(e.get(AGGMODE)!= null && e.get(AGGMODE).toString().equalsIgnoreCase(SUM)){
	        			   baseCassandraDao.saveLongValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),localKey+SEPERATOR+key+SEPERATOR+entry.getKey().toString(), eventMap.get(GOORUID), 1L);
	        			   
	        			   long sumOf = baseCassandraDao.getCount(ColumnFamily.MICROAGGREGATION.getColumnFamily(), localKey+SEPERATOR+key+SEPERATOR+entry.getKey().toString());
		                   baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,key+SEPERATOR+entry.getKey().toString(), sumOf);
		               }
	                        
	        	}
				if(eventMap.containsKey(TYPE) && eventMap.get(TYPE).equalsIgnoreCase(STOP)){
					String	collectionStatus = "completed";
					try {
						Thread.sleep(200);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+"completion_progress",collectionStatus, m);
				}
	        }
    	}
    	try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }
    
    public void startCounters(Map<String,String> eventMap,String aggregatorJson,List<String> keysList,String key) throws JSONException{    	
    	JSONObject j = new JSONObject(aggregatorJson);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
    	
    	for (Entry entry : entrySet) {
        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
	        for(String localKey : keysList){
	        	if(e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(COUNTER)){
	        		if(!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE)) &&!(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && eventMap.get(TYPE).equalsIgnoreCase(STOP)) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
	        			baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,key+SEPERATOR+entry.getKey(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
				}	
	        		
	        		if(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && (eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())) && eventMap.get(TYPE).equalsIgnoreCase(STOP)){

	        			ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,0);
	        			
	        			long views = counterColumns.getColumnByName(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName()) != null ? counterColumns.getLongValue(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName(), 0L) : 0L;
	        			long timeSpent = counterColumns.getColumnByName(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName()) != null ? counterColumns.getLongValue(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName(), 0L) : 0L;
	        			
	        			if(views == 0L && timeSpent > 0L){
	        				baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,key+SEPERATOR+entry.getKey(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
	        			}
	        		}
	        		
	        		if(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE) && eventMap.get(RESOURCETYPE).equalsIgnoreCase(QUESTION) && eventMap.get(TYPE).equalsIgnoreCase(STOP)){
	    			
	        			int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get(ATTMPTTRYSEQ)) ;
	    				int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get(ATTMPTSTATUS)) ;
	    				String answerStatus = null;
	    				int status = 0;
	    				
	    				status = Integer.parseInt(eventMap.get("attemptCount"));
    					if(status != 0){
					         status = status-1;
    					}
    					
						if(attempStatus[status] == 1){
							answerStatus = LoaderConstants.CORRECT.getName();
						}else if(attempStatus[status] == 0){
							answerStatus = LoaderConstants.INCORRECT.getName();
						}
	    	    		
						String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[status]);
	    	    		if(option != null && option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){
	    	    			answerStatus = 	option;
	    	    		}
	    				String openEndedText = eventMap.get(TEXT);
	    				if(eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE) && openEndedText != null && !openEndedText.isEmpty()){
	    					option = "A";
	    				}
	    				boolean answered = this.isUserAlreadyAnswered(localKey, key);
        				
	    				if(answered){
	    					if(!option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){
	    						baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey ,key+SEPERATOR+option,e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
	        					updatePostAggregator(localKey,key+SEPERATOR+option);
	    					}
	    				}else{
	    					baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey ,key+SEPERATOR+option,e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
        					updatePostAggregator(localKey,key+SEPERATOR+option);
	    				}
        				
        				if(eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE) && answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){	    					
        					baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey ,key+SEPERATOR+answerStatus,1L,m);
        					updatePostAggregator(localKey,key+SEPERATOR+answerStatus);
        				}
        				else if(answerStatus != null && !answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){	    					
        					baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey ,key+SEPERATOR+answerStatus,1L,m);
        					updatePostAggregator(localKey,key+SEPERATOR+answerStatus);
        				}
					}
	        		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()) && e.get(AGGMODE) != null){
		        		baseCassandraDao.saveLongValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),localKey,key+SEPERATOR+eventMap.get(GOORUID)+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
		        		baseCassandraDao.saveLongValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),localKey+SEPERATOR+key,eventMap.get(GOORUID)+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
		        		baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,key+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
	        		}
	        	}				
	        }
    	}
    	try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }

    private void updatePostAggregator(String key,String columnName){
    	Column<String> values = baseCassandraDao.readWithKeyColumn(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), key, columnName,0);    	
    	long value =  values != null ? values.getLongValue() : 0L; 
    	baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),key, columnName,value);
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

		result = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), key,0);
		
		if (result != null && !result.isEmpty() && result.getColumnByName(metric) != null) {
			count = result.getColumnByName(metric).getLongValue();
    	}
		
    	return (count);
	}

	public void realTimeAggregator(String keyValue,Map<String,String> eventMap) throws JSONException{

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		String resourceType = eventMap.get(RESOURCETYPE);
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			long scoreInPercentage = 0L;
			long score =  0L;
			String collectionStatus = "in-progress";
			if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){
				 score = eventMap.get(SCORE) != null ? Long.parseLong(eventMap.get(SCORE).toString()) : 0L; 
				if(questionCountInQuiz != 0L){
					scoreInPercentage = ((score * 100/questionCountInQuiz));
				}
			}

			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, COLLECTION+ SEPERATOR+GOORUOID,eventMap.get(CONTENTGOORUOID), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+"completion_progress",collectionStatus, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+QUESTION_COUNT,questionCountInQuiz, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE_IN_PERCENTAGE,scoreInPercentage, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE,score, m);
		}
		
		// For user feed back
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
			if(eventMap.get(SESSION).equalsIgnoreCase("AS")) {
				String sessionKey = null;
				String sessionId = null;
				String newKey = null;
				if((eventMap.get(CLASSPAGEGOORUOID) != null) && (!eventMap.get(CLASSPAGEGOORUOID).isEmpty())) {
					sessionKey = "RS"+SEPERATOR+eventMap.get(CLASSPAGEGOORUOID)+SEPERATOR+eventMap.get(PARENTGOORUOID);
				}else if((eventMap.get("classId") != null) && (!eventMap.get("classId").isEmpty())) {
					sessionKey = "RS"+SEPERATOR+eventMap.get("classId")+SEPERATOR+eventMap.get(PARENTGOORUOID);
				}else {
					sessionKey = "RS"+SEPERATOR+eventMap.get(PARENTGOORUOID);
				}
				logger.info("sessionKey:" + sessionKey);
				Column<String> session = baseCassandraDao.readWithKeyColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), sessionKey,eventMap.get(GOORUID),0);
				sessionId = session != null ? session.getStringValue() : null;
				
				if((sessionId != null) && (!sessionId.isEmpty())) {
					newKey = keyValue.replaceFirst("AS~", sessionId+SEPERATOR);
					logger.info("newKey:" + newKey);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+ SEPERATOR+FEEDBACK, eventMap.containsKey(TEXT) ? eventMap.get(TEXT) : null, m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+FEEDBACKPROVIDER,eventMap.containsKey(PROVIDER) ? eventMap.get(PROVIDER) : null, m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+TIMESTAMP,Long.valueOf(eventMap.get(STARTTIME)), m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+ACTIVE,eventMap.containsKey(ACTIVE) ? eventMap.get(ACTIVE) : "false", m);
					
				}
			}
			logger.info("keyValue : " + keyValue);
			logger.info("eventMap : " + eventMap);
			
			logger.info("\n eventMap : " + eventMap.get(ACTIVE));
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+ SEPERATOR+FEEDBACK, eventMap.containsKey(TEXT) ? eventMap.get(TEXT) : null, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+FEEDBACKPROVIDER,eventMap.containsKey(PROVIDER) ? eventMap.get(PROVIDER) : null, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+TIMESTAMP,Long.valueOf(eventMap.get(STARTTIME)), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+ACTIVE,eventMap.containsKey(ACTIVE) ? eventMap.get(ACTIVE) : "false", m);
		}
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+GOORUOID,eventMap.get(CONTENTGOORUOID), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, USERID,eventMap.get(GOORUID), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, CLASSPAGEID,eventMap.get(CLASSPAGEGOORUOID), m);
			
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())){

			Column<String> totalTimeSpentValues = baseCassandraDao.readWithKeyColumn(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName(),0);    	
	    	long totalTimeSpent =  totalTimeSpentValues != null ? totalTimeSpentValues.getLongValue() : 0L;
	    	
			Column<String> viewsValues = baseCassandraDao.readWithKeyColumn(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName(),0);    	
	    	long views =  viewsValues != null ? viewsValues.getLongValue() : 0L;

	    	if(views == 0L && totalTimeSpent > 0L){
	    		baseCassandraDao.increamentCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName(),1L);
	    		views = 1;
	    	}
	    	
			if(views != 0L){
				baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName(),totalTimeSpent, m);
				baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.AVGTS.getName(),(totalTimeSpent/views), m);
			}
		}
			if(resourceType != null && resourceType.equalsIgnoreCase(QUESTION)){		 
					if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){
    					int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get(ATTMPTTRYSEQ)) ;
    					int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get(ATTMPTSTATUS)) ;
    					String answerStatus = null;
    					int status = 0;
    					status = Integer.parseInt(eventMap.get("attemptCount"));
    					if(status != 0){
    						status = status-1;
    					}
    					int attemptStatus = attempStatus[status];
    					String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[status]);
    		    		if(option != null && option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){
    		    			answerStatus = 	option;
    		    		}
    					String textValue = null;
    					if(eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE)){
    						String openEndedtextValue = eventMap.get(TEXT);
    						if(openEndedtextValue != null && !openEndedtextValue.isEmpty()){
    							option = "A";
    						}
    					}else {
    						option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 :attemptTrySequence[status]);
    					}
    					boolean answered = this.isUserAlreadyAnswered(keyValue, eventMap.get(CONTENTGOORUOID));
    					if(answered){
    						if(!option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){
    							m = this.addObjectForAggregator(eventMap, keyValue, m,option,attemptStatus);
    						}
    					}else{
    						m = this.addObjectForAggregator(eventMap, keyValue, m,option,attemptStatus);
    					}
				      baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+TYPE ,eventMap.get(QUESTIONTYPE), m);
				      
				      
					}      				     
				}
			 try{
	         	m.execute();
	         } catch (ConnectionException e) {
	         	logger.info("Error while inserting to cassandra - JSON - ", e);
	         }
			
	}
	
	public MutationBatch addObjectForAggregator(Map<String,String> eventMap , String keyValue , MutationBatch m,String options , int attemptStatus) throws JSONException{
		
		String textValue = null ;
		String answerObject = null;
		long scoreL = 0L;
		
		textValue = eventMap.get(TEXT);
		if(eventMap.containsKey(ANSWEROBECT)){
			answerObject = eventMap.get(ANSWEROBECT).toString();
		}
		String answers = eventMap.get(ANS);
		JSONObject answersJson = new JSONObject(answers);
		JSONArray names = answersJson.names();
		String firstChoosenAns = null;
		
		
		if(names != null && names.length() != 0){
			firstChoosenAns = names.getString(0);
		}
		
		if(eventMap.get(SCORE) != null){
			scoreL = Long.parseLong(eventMap.get(SCORE).toString());
		}
		
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+CHOICE,textValue, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+ACTIVE,"false", m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE,scoreL, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+OPTIONS,options, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+STATUS,Long.valueOf(attemptStatus), m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+ANSWER_OBECT,answerObject, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID) + SEPERATOR+CHOICE,firstChoosenAns, m);
		
		return m;								
	}

	private List<String> getClassPagesFromItems(List<String> parentIds){
		List<String> classPageGooruOids = new ArrayList<String>();
		for(String classPageGooruOid : parentIds){
			String type = null;
			ColumnList<String>  resource = baseCassandraDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(),"GLP~"+classPageGooruOid,0);
			if(resource != null){	
				type =  resource.getColumnByName("type_name") != null ? resource.getColumnByName("type_name").getStringValue() : null ;
			}
			
			if(type != null && type.equalsIgnoreCase(LoaderConstants.CLASSPAGE.getName())){
				classPageGooruOids.add(classPageGooruOid);
			}
		}
		return classPageGooruOids;
	}
	
	public long getQuestionCount(Map<String,String> eventMap) {
		String contentGooruOId = eventMap.get(CONTENTGOORUOID);
		ColumnList<String> questionLists = null;
		long totalQuestion = 0L;
		long oeQuestion = 0L;
		long updatedQuestionCount = 0L;

		questionLists = baseCassandraDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), contentGooruOId,0);
			
			if((questionLists != null) && (!questionLists.isEmpty())){
					totalQuestion =  questionLists.getColumnByName("questionCount").getLongValue();
					oeQuestion =   questionLists.getColumnByName("oeCount").getLongValue();
					updatedQuestionCount = totalQuestion - oeQuestion;
			}
    	return updatedQuestionCount;
	}
	
	private boolean isRowAvailable(String key,String  columnName,String currentSession){
		ColumnList<String>  result = null;
		result = baseCassandraDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), key,0);
		String storedSession = result.getStringValue(columnName, null);
		if (storedSession != null && !storedSession.equalsIgnoreCase(currentSession)) {
			return true;
    	}		
		return false;
		
	}
	
	public List<String> getClassPages(Map<String,String> eventMap){
    	List<String> classPages = new ArrayList<String>();
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(PARENTGOORUOID) == null){
    		List<String> parents = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(CONTENTGOORUOID),0);
    		if(!parents.isEmpty()){    			
    			classPages = this.getClassPagesFromItems(parents);
    		}
    	}else if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
    		classPages.add(eventMap.get(PARENTGOORUOID));
    	}
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.get(PARENTGOORUOID) != null){
    		if(eventMap.get(CLASSPAGEGOORUOID) == null){
	    		ColumnList<String> eventDetail = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventMap.get(PARENTEVENTID),0);
		    	if(eventDetail != null && eventDetail.size() > 0){
		    		if(eventDetail.getStringValue(EVENT_NAME, null) != null &&  (eventDetail.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			    		if(eventDetail.getStringValue(PARENT_GOORU_OID, null) == null || eventDetail.getStringValue(PARENT_GOORU_OID, null).isEmpty()){
			    			List<String> parents = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventDetail.getStringValue(CONTENT_GOORU_OID, null),0);
			    			if(!parents.isEmpty()){    			
			        			classPages = this.getClassPagesFromItems(parents);
			        		}
			    		}else{
			    			classPages.add(eventDetail.getStringValue(PARENT_GOORU_OID, null));
			    		}
		    		}
		    	}else{
		    		List<String> parents = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(PARENTGOORUOID),0);
		    		if(!parents.isEmpty()){    			
		    			classPages = this.getClassPagesFromItems(parents);
		    		}
		    	}
    		}
    	}
	    	if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()) && eventMap.get(CLASSPAGEGOORUOID) == null)){
	    		if(eventMap.get(CLASSPAGEGOORUOID) == null){
	            ColumnList<String> R = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventMap.get(PARENTEVENTID),0);
	            if(R != null && R.size() > 0){
	            	String parentEventId = R.getStringValue(PARENT_EVENT_ID, null);
	            	if(parentEventId != null ){
	            		ColumnList<String> C = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),parentEventId,0);
	            		if(C.getStringValue(EVENT_NAME, null) != null && (C.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CLPV1.getName())){
			    			classPages.add(C.getStringValue(CONTENT_GOORU_OID, null));
			    		}
			    		if(C.getStringValue(EVENT_NAME, null) != null &&  (C.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				    		if(C.getStringValue(PARENT_GOORU_OID, null) == null || C.getStringValue(PARENT_GOORU_OID, null).isEmpty()){
				    			List<String> parents = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(),C.getStringValue(CONTENT_GOORU_OID, null),0);
				    			if(!parents.isEmpty()){    			
				        			classPages = this.getClassPagesFromItems(parents);
				        		}
				    		}else{
				    			classPages.add(C.getStringValue(PARENT_GOORU_OID, null));
				    		}
			    		}
	            	}
	            }
	        }
    	}
	    if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
	    	if(eventMap.containsKey("classId") && StringUtils.isNotBlank(eventMap.get("classId")) ){
	    		classPages.add(eventMap.get("classId"));
	    	}
	    }
	    	
	    	if(eventMap.containsKey(CLASSPAGEGOORUOID) && eventMap.get(CLASSPAGEGOORUOID) != null){
	    		classPages.add(eventMap.get(CLASSPAGEGOORUOID));
	    	}
	    	
	    	return classPages;
	}

	private long iterateAndFindAvg(String key){
		ColumnList<String> columns = null;
		long values = 0L;
		long count = 0L; 
		long avgValues = 0L;

		columns = baseCassandraDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), key,0);
		count = columns.size();
		if(columns != null && columns.size() > 0){
			for(int i = 0 ; i < columns.size() ; i++) {
				values += columns.getColumnByIndex(i).getLongValue();
			}
			avgValues = values/count;
		}
		
		return avgValues;
	}
	

	private void calculateAvg(String localKey,String divisor,String dividend,String columnToUpdate){
		long d = this.getCounterLongValue(localKey, divisor);
	    	if(d != 0L){
	    		long average = (this.getCounterLongValue(localKey, dividend)/d);
	    		baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(),localKey,columnToUpdate, average);
	    	}
    	}


	public boolean isUserAlreadyAnswered(String key,String columnPrefix){
		ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),key,0);
		boolean status= false;
		long attemptCount = counterColumns.getColumnByName(columnPrefix+SEPERATOR+ATTEMPTS) != null ? counterColumns.getLongValue(columnPrefix+SEPERATOR+ATTEMPTS, null) : 0L;
		
		if(attemptCount > 0L){
				status = true;
		}
		
		return status;
		
	}
	public void migrationAndUpdate(Map<String,String> eventMap){
    	List<String> classPages = this.getClassPages(eventMap);
    	String key = eventMap.get(CONTENTGOORUOID);
		List<String> keysList = new ArrayList<String>();
				

		if(eventMap.get(MODE) != null && eventMap.get(MODE).equalsIgnoreCase(STUDY)){

			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
					boolean isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classPage,0);
					if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID));
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					}
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				}
			}
			
		}

		this.completeMigration(eventMap, keysList);
	}
	
	public void completeMigration(Map<String,String> eventMap,List<String> keysList){
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if(keysList != null && keysList.size() > 0 ){
			for(String keyValue : keysList){	
				ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),keyValue,0);
    			long views = counterColumns.getColumnByName(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.VIEWS.getName()) != null ? counterColumns.getLongValue(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.VIEWS.getName(), 0L) : 0L;
    			long timeSpent = counterColumns.getColumnByName(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName()) != null ? counterColumns.getLongValue(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName(), 0L) : 0L;
    			logger.info("views : {} : timeSpent : {} ",views,timeSpent);
    			
    			if(views == 0L && timeSpent > 0L ){
    				baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),keyValue,eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.VIEWS.getName(), 1L ,m);
    			}
			}
		}
	}
	@SuppressWarnings("unchecked")
	@Async
	public void updateRawData(Map<String, Object> eventMap) {
		logger.info("Into Raw Data Update");

		Map<String, Object> eventDataMap = new HashMap<String, Object>();
		List<Map<String, Object>> eventDataMapList = new ArrayList<Map<String, Object>>();
		if ((eventMap.containsKey(DATA))) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				try {
					eventDataMap = mapper.readValue(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {
					});
				} catch (Exception e) {
					eventDataMapList = mapper.readValue(eventMap.get(DATA).toString(), new TypeReference<ArrayList<Map<String, Object>>>() {
					});
				}
			} catch (Exception e1) {
				logger.error("Unable to parse data object inside payloadObject ", e1);
			}
		}

		if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMCREATE.getName()) 
				&& ((eventMap.containsKey(DATA)) && eventDataMap != null && !eventDataMap.isEmpty())
				&& (eventMap.containsKey(ITEMTYPE) && (eventMap.get(ITEMTYPE).toString().matches(ITEMTYPES_SCSFFC)) && eventMap.containsKey(MODE))) {
			/** Collection Create **/

			if (eventMap.get(MODE).toString().equalsIgnoreCase(CREATE)
					 && (eventDataMap.containsKey(RESOURCETYPE) && ((((Map<String, String>) eventDataMap.get(RESOURCETYPE)).get(NAME)
							.toString().matches(RESOURCETYPES_SF))))) {
				this.generateCollectionCreate(eventDataMap, eventMap);
			} else if (eventMap.get(MODE).toString().equalsIgnoreCase(MOVE)) {
				this.generateCollectionMove(eventDataMap, eventMap);
			} else if (eventMap.get(MODE).toString().equalsIgnoreCase(COPY)) {
				this.generateCollectionCopy(eventDataMap, eventMap);
			}
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMCREATE.getName())
				&& (eventMap.containsKey(ITEMTYPE) && (eventMap.get(ITEMTYPE).toString().equalsIgnoreCase(CLASSPAGE)))
				&& ((eventMap.containsKey(DATA)) && !eventDataMap.isEmpty() && (eventDataMap.containsKey(RESOURCETYPE)) && ((Map<String, String>) eventDataMap.get(RESOURCETYPE)).get(NAME).toString()
						.equalsIgnoreCase(CLASSPAGE))) {
				this.generateClasspageCreate(eventDataMap, eventMap);
				
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.CLUAV1.getName()) && eventMap.containsKey(DATA)) {
			
			this.generateClasspageUserAdd(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.CPUSERREMOVE.getName())) {
			
			this.generateClasspageUserRemove(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMCREATE.getName())
				&& (eventMap.containsKey(ITEMTYPE) && (eventMap.get(ITEMTYPE).toString().equalsIgnoreCase("classpage.collection"))) 
				&& eventMap.containsKey(DATA) && eventDataMapList.size() > 0) {
			
			this.generateClasspageAssignmentCreate(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMEDIT.getName())
				&& (eventMap.containsKey(ITEMTYPE) && (eventMap.get(ITEMTYPE).toString().equalsIgnoreCase("classpage.collection")))
				&& ((eventMap.containsKey(DATA)) && !eventDataMap.isEmpty() && eventDataMap.containsKey(RESOURCE)
						&& ((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE)).containsKey(RESOURCETYPE) && (((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE))
						.get(RESOURCETYPE).get(NAME).toString().equalsIgnoreCase("scollection")))) {
			
			this.generateClasspageAssignmentEdit(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMEDIT.getName())
				&& (eventMap.containsKey(ITEMTYPE) && ((eventMap.get(ITEMTYPE).toString().equalsIgnoreCase("shelf.collection")) || eventMap.get(ITEMTYPE).toString().equalsIgnoreCase("classpage")))
				&& (eventMap.containsKey(DATA))) {
			
			this.generateShelfCollectionOrClasspageEdit(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMCREATE.getName())
				&& (eventMap.containsKey(ITEMTYPE) && (eventMap.get(ITEMTYPE).toString().equalsIgnoreCase("collection.resource")))
				&& ((Map<String, Object>) eventDataMap.get(RESOURCE)).containsKey(RESOURCETYPE)
				&& (!(((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE)).get(RESOURCETYPE).get(NAME).toString().equalsIgnoreCase("scollection")) && !((Map<String, Map<String, Object>>) eventDataMap
						.get(RESOURCE)).get(RESOURCETYPE).get(NAME).toString().equalsIgnoreCase("classpage"))) {
			
			this.generateResourceCreate(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMEDIT.getName())
				&& (eventMap.containsKey(ITEMTYPE) && (eventMap.get(ITEMTYPE).toString().equalsIgnoreCase("collection.resource")))
				&& ((Map<String, Object>) eventDataMap.get(RESOURCE)).containsKey(RESOURCETYPE)
				&& (!(((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE)).get(RESOURCETYPE).get(NAME).toString().equalsIgnoreCase("scollection")) && !((Map<String, Map<String, Object>>) eventDataMap
						.get(RESOURCE)).get(RESOURCETYPE).get(NAME).toString().equalsIgnoreCase(CLASSPAGE))) {
			
			this.generateResourceEdit(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.SCITEMDELETE.getName())) {
			
			this.markItemDelete(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.REGISTERUSER.getName())) {
			
			this.generateRegisterUser(eventDataMap, eventMap);
			
		} else if (eventMap.get(EVENTNAME).toString().equalsIgnoreCase(LoaderConstants.PROFILEACTION.getName()) && eventMap.get(ACTIONTYPE).toString().equalsIgnoreCase(EDIT)) {
			
			this.generateProfileEdit(eventDataMap, eventMap);
			
		}
	}
	
	private void generateCollectionCreate(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		ResourceCo collection = new ResourceCo();
		Map<String, Object> collectionMap = new HashMap<String, Object>();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {});
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			collection.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
			collectionMap.put(CONTENTID, Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		try {
			baseCassandraDao.updateResourceEntity(rawUpdateDAO.updateCollection(dataMap, collection));
		} catch (Exception ex) {
			logger.error("Unable to save resource entity for Id {} due to {}", dataMap.get("gooruOid").toString(), ex);
		}
		/** Update insights collection CF for collection mapping **/
		rawUpdateDAO.updateCollectionTable(dataMap, collectionMap);
		
		/**Update Insights colectionItem CF for shelf-collection/folder-collection/shelf-folder mapping**/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (entry.getKey().equalsIgnoreCase(ITEMTYPE)) {
				collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
			} else if (entry.getKey().equalsIgnoreCase("ItemId") || entry.getKey().equalsIgnoreCase(COLLECTIONITEMID)){
				collectionItemMap.put(COLLECTIONITEMID, (eventMap.containsKey("ItemId") && eventMap.get("ItemId") != null) ? eventMap.get("ItemId").toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put("deleted", Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
	}
	
	private void generateCollectionMove(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		/** Update Insights colectionItem CF for folder-collection mapping **/
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {
		});
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase("associationDate")) {
				collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
			} else if (entry.getKey().equalsIgnoreCase(ITEMSEQUENCE)) {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? Integer.valueOf(eventMap.get(entry.getKey()).toString()) : null);
			} else {
				collectionItemMap.put(entry.getValue(), ((eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null));
			}
		}
		collectionItemMap.put("deleted", Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);

		Map<String, Object> markSourceItemMap = new HashMap<String, Object>();
		markSourceItemMap.put("collectionItemId", ((eventMap.containsKey("sourceItemId") && eventMap.get("sourceItemId") != null) ? eventMap.get("sourceItemId").toString() : null));
		markSourceItemMap.put("deleted", Integer.valueOf(1));
		rawUpdateDAO.updateCollectionItemTable(eventMap, markSourceItemMap);

	}
	
	private void generateCollectionCopy(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Collection Copy **/

		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {
		});
		Map<String, Object> collectionMap = new HashMap<String, Object>();
		ResourceCo collection = new ResourceCo();
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			collection.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
			collectionMap.put(CONTENTID, Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		baseCassandraDao.updateResourceEntity(rawUpdateDAO.updateCollection(dataMap, collection));

		/** Update insights collection CF for collection mapping **/
		rawUpdateDAO.updateCollectionTable(dataMap, collectionMap);

		/** Update Insights colectionItem CF for shelf-collection mapping **/
		String collectionGooruOid = dataMap.containsKey("gooruOid") ? dataMap.get("gooruOid").toString() : null;
		if (collectionGooruOid != null) {
			List<Map<String, Object>> collectionItemList = (List<Map<String, Object>>) dataMap.get("collectionItems");
			for (Map<String, Object> collectionItem : collectionItemList) {
				Map<String, Object> collectionItemMap = new HashMap<String, Object>();
				Map<String, Object> collectionItemResourceMap = (Map<String, Object>) collectionItem.get("resource");
				Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
				for(Entry<String, String> entry : entrySet) {
					if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase("associationDate")) {
						collectionItemMap.put(entry.getValue(), (collectionItem.containsKey(entry.getKey()) && collectionItem.get(entry.getKey()) != null) ? collectionItem.get(entry.getKey()).toString() : null);
					} else if(entry.getKey().equalsIgnoreCase("questionType")){
						collectionItemMap.put(entry.getValue(),
								((collectionItemResourceMap.containsKey(entry.getKey()) && collectionItemResourceMap.get(entry.getKey()) != null) ? 
										collectionItemResourceMap.get(entry.getKey()).toString() : null));
					} else if(entry.getKey().equalsIgnoreCase(CONTENTGOORUOID)){
						collectionItemMap.put(entry.getValue(), (collectionItemResourceMap.containsKey("gooruOid") ? collectionItemResourceMap.get("gooruOid").toString() : null));
					} else if(entry.getKey().equalsIgnoreCase(PARENTGOORUOID)){
						collectionItemMap.put(entry.getValue(), (eventMap.containsKey(CONTENTGOORUOID) ? eventMap.get(CONTENTGOORUOID).toString() : null));
					} else {
						collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
					}
				}
				collectionItemMap.put("deleted", Integer.valueOf(0));
				rawUpdateDAO.updateCollectionItemTable(collectionItem, collectionItemMap);
			}
		}
	}
	
	private void generateClasspageCreate(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** classpage create **/

		ResourceCo collection = new ResourceCo();
		Map<String, Object> collectionMap = new HashMap<String, Object>();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {});
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			collection.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
			collectionMap.put(CONTENTID, Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		try {
			baseCassandraDao.updateResourceEntity(rawUpdateDAO.updateCollection(dataMap, collection));
		} catch (Exception ex) {
			logger.error("Unable to save resource entity for Id {} due to exception {}", dataMap.get("gooruOid").toString(), ex);
		}

		Map<String, Object> classpageMap = new HashMap<String, Object>();
		classpageMap.put("classCode", ((eventMap.containsKey("classCode") && eventMap.get("classCode") != null) ? eventMap.get("classCode").toString() : null));
		classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
		classpageMap.put(CONTENTID, ((eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) ? Long.valueOf(eventMap.get(CONTENTID).toString()) : null));
		classpageMap.put("organizationUId", ((eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null));
		classpageMap.put("userUid", ((eventMap.containsKey(GOORUID) && eventMap.get(GOORUID) != null) ? eventMap.get(GOORUID).toString() : null));
		classpageMap.put("isGroupOwner", 1);
		classpageMap.put("deleted", Integer.valueOf(0));
		classpageMap.put("activeFlag", 1);
		classpageMap.put("userGroupType", SYSTEM);
		rawUpdateDAO.updateClasspage(dataMap, classpageMap);
		
		/** Update insights collection CF for collection mapping **/
		rawUpdateDAO.updateCollectionTable(dataMap, collectionMap);

		/** Update Insights colectionItem CF for shelf-collection mapping **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (entry.getKey().equalsIgnoreCase(ITEMTYPE)) {
				collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
			} else if (entry.getKey().equalsIgnoreCase(COLLECTIONITEMID)){
				collectionItemMap.put(COLLECTIONITEMID, (eventMap.containsKey("ItemId") && eventMap.get("ItemId") != null) ? eventMap.get("ItemId").toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put("deleted", Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
	}
	
	private void generateClasspageUserAdd(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		/** classpage user add **/
		List<Map<String, Object>> dataMapList = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<ArrayList<Map<String, Object>>>() {});
		for (Map<String, Object> dataMap : dataMapList) {
			Map<String, Object> classpageMap = new HashMap<String, Object>();
			classpageMap.put("classId", ((eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null) ? eventMap.get(CONTENTGOORUOID).toString() : null));
			classpageMap.put(CLASSCODE, ((eventMap.containsKey(CLASSCODE) && eventMap.get(CLASSCODE) != null) ? eventMap.get(CLASSCODE).toString() : null));
			classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
			classpageMap.put(CONTENTID, ((eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) ? Long.valueOf(eventMap.get(CONTENTID).toString()) : null));
			classpageMap.put(ORGANIZATIONUID, ((eventMap.containsKey(ORGANIZATIONUID) && eventMap.get(ORGANIZATIONUID) != null) ? eventMap.get(ORGANIZATIONUID).toString() : null));
			classpageMap.put("isGroupOwner",  Integer.valueOf(0));
			classpageMap.put("deleted", Integer.valueOf(0));
			classpageMap.put("activeFlag", (dataMap.containsKey("status") && dataMap.get("status") != null && dataMap.get("status").toString().equalsIgnoreCase("active")) ? Integer.valueOf(1) : Integer.valueOf(0));
			classpageMap.put("username", ((dataMap.containsKey("username") && dataMap.get("username") != null) ? dataMap.get("username") : null));
			classpageMap.put("userUid", ((dataMap.containsKey("gooruUid") && dataMap.get("gooruUid") != null) ? dataMap.get("gooruUid") : null));
			baseCassandraDao.updateClasspageCF(ColumnFamily.CLASSPAGE.getColumnFamily(), classpageMap);
		}
	}
	private void generateClasspageUserRemove(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		Map<String, Object> classpageMap = new HashMap<String, Object>();
		classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
		classpageMap.put("deleted", Integer.valueOf(1));
		classpageMap.put("classId", ((eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null) ? eventMap.get(CONTENTGOORUOID).toString() : null));
		classpageMap.put("userUid", ((eventMap.containsKey("removedGooruUId") && eventMap.get("removedGooruUId") != null) ? eventMap.get("removedGooruUId") : null));
		baseCassandraDao.updateClasspageCF(ColumnFamily.CLASSPAGE.getColumnFamily(), classpageMap);
	}
	private void generateClasspageAssignmentCreate(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		/** classpage assignment create **/
		List<Map<String, Object>> dataMapList = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<ArrayList<Map<String, Object>>>() {});
		for (Map<String, Object> dataMap : dataMapList) {
			if (((eventMap.containsKey(DATA)) && ((Map<String, Object>) dataMap.get(RESOURCE)).containsKey(RESOURCETYPE) && (((Map<String, String>) ((Map<String, Object>) dataMap.get(RESOURCE))
					.get(RESOURCETYPE)).get(NAME).toString().equalsIgnoreCase("scollection")))) {
				/** Update insights collection CF for collection mapping **/
				Map<String, Object> resourceMap = (Map<String, Object>) dataMap.get(RESOURCE);
				Map<String, Object> collectionMap = new HashMap<String, Object>();
				if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
					collectionMap.put("contentId", Long.valueOf(eventMap.get(CONTENTID).toString()));
				}
				collectionMap.put("gooruOid", eventMap.get(PARENTGOORUOID));
				rawUpdateDAO.updateCollectionTable(resourceMap, collectionMap);

				/** Update Insights colectionItem CF for shelf-collection mapping **/
				Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
				Map<String, Object> collectionItemMap = new HashMap<String, Object>();
				for(Entry<String, String> entry : entrySet) {
					if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase("associationDate")) {
						collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
					} else {
						collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
					}
				}
				collectionItemMap.put("deleted", Integer.valueOf(0));
				rawUpdateDAO.updateCollectionItemTable(dataMap, collectionItemMap);
			}
		}
	}
	private void generateClasspageAssignmentEdit(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Update Insights colectionItem CF for shelf-collection mapping **/
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {});
		Map<String, Object> resourceMap = (Map<String, Object>) dataMap.get(RESOURCE);
		ResourceCo collection = new ResourceCo();
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			collection.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		try {
			baseCassandraDao.updateResourceEntity(rawUpdateDAO.updateCollection(resourceMap, collection));
		} catch (Exception ex) {
			logger.error("Unable to save resource entity for Id {} due to {}", dataMap.get("gooruOid").toString(), ex);
		}
		
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase("associationDate")) {
				collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put("deleted", Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(dataMap, collectionItemMap);
	}
	
	private void generateShelfCollectionOrClasspageEdit(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		/** Collection/Classpage Edit **/
		
		ResourceCo collection = new ResourceCo();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {
		});
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			collection.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		baseCassandraDao.updateResourceEntity(rawUpdateDAO.updateCollection(dataMap, collection));
	}
	private void generateResourceCreate(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Resource Create **/
		
		/** Update Resource CF for resource data **/
		ResourceCo resourceCo = new ResourceCo();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {});
		rawUpdateDAO.updateResource(dataMap, resourceCo);
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			resourceCo.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		baseCassandraDao.updateResourceEntity(resourceCo);

		Map<String, Object> resourceMap = (Map<String, Object>) dataMap.get(RESOURCE);

		if (dataMap.containsKey(COLLECTION) && dataMap.get(COLLECTION) != null && ((Map<String, Map<String, String>>) dataMap.get(COLLECTION)).containsKey(RESOURCETYPE)
				&& (((Map<String, Map<String, String>>) dataMap.get(COLLECTION)).get(RESOURCETYPE).get(NAME).equalsIgnoreCase("scollection"))) {
			Map<String, Object> collectionMap = (Map<String, Object>) dataMap.get(COLLECTION);
			/** Update Resource CF for collection data **/
			ResourceCo collection = new ResourceCo();
			rawUpdateDAO.updateCollection(collectionMap, collection);
			collection.setContentId(Long.valueOf(eventMap.get(PARENTCONTENTID).toString()));
			collection.setVersion(Integer.valueOf(collectionMap.get(VERSION).toString()));
			baseCassandraDao.updateResourceEntity(collection);
		}
		
		/** Update Insights colectionItem CF **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase("associationDate")) {
				collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put("deleted", Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(resourceMap, collectionItemMap);
		
		/** Update Insights Assessment Answer CF **/
		if (((Map<String, String>) resourceMap.get(RESOURCETYPE)).get(NAME).equalsIgnoreCase("assessment-question")) {
			Map<String, Object> assessmentAnswerMap = new HashMap<String, Object>();
			String collectionGooruOid = eventMap.get("parentGooruId").toString();
			String questionGooruOid = eventMap.get("contentGooruId").toString();
			Long collectionContentId = ((eventMap.containsKey(PARENTCONTENTID) && eventMap.get(PARENTCONTENTID) != null) ? Long.valueOf(eventMap.get(PARENTCONTENTID).toString()) : null);
			Long questionId = ((eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) ? Long.valueOf(eventMap.get(CONTENTID).toString()) : null);
			assessmentAnswerMap.put("collectionGooruOid", collectionGooruOid);
			assessmentAnswerMap.put("questionGooruOid", questionGooruOid);
			assessmentAnswerMap.put("questionId", questionId);
			assessmentAnswerMap.put("collectionContentId", collectionContentId);
			rawUpdateDAO.updateAssessmentAnswer(resourceMap, assessmentAnswerMap);
		}
	}
	private void generateResourceEdit(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Resource Edit **/

		ResourceCo resourceCo = new ResourceCo();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {});
		Map<String, Object> resourceMap = (Map<String, Object>) dataMap.get(RESOURCE);
		if (eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) {
			resourceCo.setContentId(Long.valueOf(eventMap.get(CONTENTID).toString()));
		}
		baseCassandraDao.updateResourceEntity(rawUpdateDAO.updateResource(dataMap, resourceCo));
		if (dataMap.containsKey(COLLECTION) && dataMap.get(COLLECTION) != null && ((Map<String, Map<String, String>>) dataMap.get(COLLECTION)).containsKey(RESOURCETYPE)
				&& (((Map<String, Map<String, String>>) dataMap.get(COLLECTION)).get(RESOURCETYPE).get(NAME).equalsIgnoreCase("scollection"))) {
			Map<String, Object> collectionMap = (Map<String, Object>) dataMap.get(COLLECTION);
			ResourceCo collection = new ResourceCo();
			rawUpdateDAO.updateCollection(collectionMap, collection);
			collection.setContentId(Long.valueOf(eventMap.get(PARENTCONTENTID).toString()));
			collection.setVersion(Integer.valueOf(collectionMap.get(VERSION).toString()));
			baseCassandraDao.updateResourceEntity(collection);
		}
		
		/** Update Insights colectionItem CF **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase("associationDate") || entry.getKey().equalsIgnoreCase("typeName")) {
				collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put("deleted", Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(resourceMap, collectionItemMap);
		
		if (((Map<String, String>) resourceMap.get(RESOURCETYPE)).get(NAME).equalsIgnoreCase("assessment-question")) {
			Map<String, Object> assessmentAnswerMap = new HashMap<String, Object>();
			String collectionGooruOid = eventMap.get(CONTENTGOORUOID).toString();
			String questionGooruOid = eventMap.get(PARENTGOORUOID).toString();
			Long collectionContentId = ((eventMap.containsKey(PARENTCONTENTID) && eventMap.get(PARENTCONTENTID) != null) ? Long.valueOf(eventMap.get(PARENTCONTENTID).toString()) : null);
			Long questionId = ((eventMap.containsKey(CONTENTID) && eventMap.get(CONTENTID) != null && !StringUtils.isBlank(eventMap.get(CONTENTID).toString())) ? Long.valueOf(eventMap.get(CONTENTID).toString()) : null);
			assessmentAnswerMap.put("collectionGooruOid", collectionGooruOid);
			assessmentAnswerMap.put("questionGooruOid", questionGooruOid);
			assessmentAnswerMap.put("questionId", questionId);
			assessmentAnswerMap.put("collectionContentId", collectionContentId);
			rawUpdateDAO.updateAssessmentAnswer((Map<String, Object>) dataMap.get("questionInfo"), assessmentAnswerMap);
		}
	
	}
	private void generateRegisterUser(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		UserCo userCo = new UserCo();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {
		});
		userCo.setAccountId((dataMap.containsKey("organizationUId") && dataMap.get("organizationUId") != null) ? dataMap.get("organizationUId").toString() : null);
		if (eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) {
			userCo.setAccountId((eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null);
			userCo.setOrganizationUid((eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null);
			Map<String, String> organizationMap = new HashMap<String, String>();
			organizationMap.put("partyUid", (eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null);
			userCo.setOrganization(organizationMap);
		}
		baseCassandraDao.updateUserEntity(rawUpdateDAO.updateUser(dataMap, userCo));
	
	}
	private void generateProfileEdit(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		UserCo userCo = new UserCo();
		Map<String, Object> dataMap = JSONDeserializer.deserialize(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {});
		userCo.setOrganizationUid((dataMap.containsKey("organizationUId") && dataMap.get("organizationUId") != null) ? dataMap.get("organizationUId").toString() : null);
		userCo.setAboutMe((dataMap.containsKey("aboutMe") && dataMap.get("aboutMe") != null) ? dataMap.get("aboutMe").toString() : null);
		userCo.setAccountId((dataMap.containsKey("organizationUId") && dataMap.get("organizationUId") != null) ? dataMap.get("organizationUId").toString() : null);
		userCo.setGrade((dataMap.containsKey("grade") && dataMap.get("grade") != null) ? dataMap.get("grade").toString() : null);
		userCo.setNetwork((dataMap.containsKey("school") && dataMap.get("school") != null) ? dataMap.get("school").toString() : null);
		userCo.setNotes((dataMap.containsKey("notes") && dataMap.get("notes") != null) ? dataMap.get("notes").toString() : null);
		if (eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) {
			userCo.setAccountId((eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null);
			userCo.setOrganizationUid((eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null);
			Map<String, String> organizationMap = new HashMap<String, String>();
			organizationMap.put("partyUid", (eventMap.containsKey("organizationUId") && eventMap.get("organizationUId") != null) ? eventMap.get("organizationUId").toString() : null);
			userCo.setOrganization(organizationMap);
		}
		baseCassandraDao.updateUserEntity(rawUpdateDAO.updateUser((Map<String, Object>) dataMap.get("user"), userCo));
	
	}
	
	private void markItemDelete(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		collectionItemMap.put(COLLECTIONITEMID, (eventMap.containsKey("ItemId") ? eventMap.get("ItemId").toString() : null));
		collectionItemMap.put("deleted", Integer.valueOf(1));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
	}
	
}
