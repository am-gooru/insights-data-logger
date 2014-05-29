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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import com.netflix.astyanax.serializers.StringSerializer;
public class CounterDetailsDAOCassandraImpl extends BaseDAOCassandraImpl implements CounterDetailsDAO,Constants {
	
    private static final Logger logger = LoggerFactory.getLogger(CounterDetailsDAOCassandraImpl.class);
    
    private final ColumnFamily<String, String> realTimeCounter;
    
    private final ColumnFamily<String, String> realTimeAggregator;
    
    private final ColumnFamily<String, String> microAggregator;
    
    private final ColumnFamily<String,String> questionCount;
    
    private static final String CF_QUESTION_COUNT = "question_count";
    
    private static final String CF_RT_COUNTER = "real_time_counter";
    
    private static final String CF_RT_AGGREGATOR = "real_time_aggregator";
    
    private static final String CF_MICRO_AGGREGATOR = "micro_aggregation";
    
    private CassandraConnectionProvider connectionProvider;
    
    private CollectionItemDAOImpl collectionItem;
    
    private EventDetailDAOCassandraImpl eventDetailDao; 
    
    private DimResourceDAOImpl dimResource;
    
    private ClasspageDAOImpl classpage;
    
    private SimpleDateFormat secondsDateFormatter;
    
    private long questionCountInQuiz = 0L;
    
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
        
        questionCount = new ColumnFamily<String, String>(
                CF_QUESTION_COUNT, // Column Family Name
         StringSerializer.get(), // Key Serializer
         StringSerializer.get()); // Column Serializer
        this.collectionItem = new CollectionItemDAOImpl(this.connectionProvider);
        this.eventDetailDao = new EventDetailDAOCassandraImpl(this.connectionProvider);
        this.dimResource = new DimResourceDAOImpl(this.connectionProvider);
        this.classpage = new ClasspageDAOImpl(this.connectionProvider);
        this.secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
    }
    
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
				generateCounter(key,contentGooruOId+SEPERATOR+VIEWS,1, m);
				generateCounter(key,gooruUId+SEPERATOR+VIEWS,1, m);
				generateCounter(key,VIEWS,1,m);
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
    public void realTimeMetrics(Map<String,String> eventMap,String aggregatorJson) throws JSONException{
    	List<String> classPages = this.getClassPages(eventMap);
    	String key = eventMap.get(CONTENTGOORUOID);
		List<String> keysList = new ArrayList<String>();
		
		boolean isStudent = false;
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(MODE).equalsIgnoreCase(STUDY) && eventMap.get(TYPE).equalsIgnoreCase(START)){
			Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
	        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
	        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
	        	/*isStudent = classpage.isUserPartOfClass(eventMap.get(GOORUID),eventMap.get(PARENTGOORUOID));
	        	if(isStudent){*/
	        		this.addSession(eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
	        	//}
	        }
			this.addSession( eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
		}
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(MODE).equalsIgnoreCase(STUDY)){
			questionCountInQuiz = this.getQuestionCount(eventMap);
			if(classPages != null && classPages.size() > 0){
				for(String classPage : classPages){
					boolean isOwner = classpage.getClassPageOwnerInfo(eventMap.get(GOORUID),classPage);
					
					logger.info("isOwner : {}",isOwner);
					
					isStudent = classpage.isUserPartOfClass(eventMap.get(GOORUID),classPage);
					
					logger.info("isStudent : {}",isStudent);
					
					eventMap.put(CLASSPAGEGOORUOID, classPage);
					if(!isOwner && isStudent){
					keysList.add(ALLSESSION+classPage+SEPERATOR+key);
					keysList.add(ALLSESSION+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					}
					
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					logger.info("Recent Key : {} ",eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(RECENTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					
					if(!this.isRowAvailable(FIRSTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
						keysList.add(FIRSTSESSION+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						this.addColumnForAggregator(FIRSTSESSION+classPage+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
					
				}
			}
				keysList.add(ALLSESSION+key);
				keysList.add(ALLSESSION+key+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
				this.addColumnForAggregator(RECENTSESSION+key, eventMap.get(GOORUID), eventMap.get(SESSION));
				if(!this.isRowAvailable(FIRSTSESSION+key, eventMap.get(GOORUID),eventMap.get(SESSION))){
					keysList.add(FIRSTSESSION+key+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(FIRSTSESSION+key, eventMap.get(GOORUID), eventMap.get(SESSION));
				}
		}

		if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.get(MODE).equalsIgnoreCase(STUDY) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()))){

			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					boolean isOwner = classpage.getClassPageOwnerInfo(eventMap.get(GOORUID),classPage);
					
					isStudent = classpage.isUserPartOfClass(eventMap.get(GOORUID),classPage);
					
					if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID));
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					}
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(RECENTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
					if(!this.isRowAvailable(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
						keysList.add(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						this.addColumnForAggregator(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
				}
			}
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID));
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				this.addColumnForAggregator(RECENTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION));
				if(!this.isRowAvailable(FIRSTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION))){
					keysList.add(FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					this.addColumnForAggregator(FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION));
				}
			
			
		}

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
			if(classPages != null && classPages.size() > 0){				
				for(String classPage : classPages){
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
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
	        			long value = this.getCounterLongValue(localKey, key+SEPERATOR+entry.getKey().toString());
	        	    	this.generateAggregator(localKey, key+SEPERATOR+entry.getKey().toString(),value,m);
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
	    						this.generateAggregator(localKey,key+SEPERATOR+option,value,m);	    						
	    					}
	    				}else{
	    					long value = this.getCounterLongValue(localKey,key+SEPERATOR+option);
    						this.generateAggregator(localKey,key+SEPERATOR+option,value,m);	    
	    				}
	    				if(!eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE) && !answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){	    					
	    					long values = this.getCounterLongValue(localKey,key+SEPERATOR+answerStatus);
		        	    	this.generateAggregator(localKey,key+SEPERATOR+answerStatus,values,m);
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
	                   this.updateRealTimeAggregator(localKey,eventMap.get(PARENTGOORUOID)+SEPERATOR+entry.getKey().toString(), averageC);
	                   long averageR = this.iterateAndFindAvg(localKey+SEPERATOR+eventMap.get(CONTENTGOORUOID));
	                   this.updateRealTimeAggregator(localKey,eventMap.get(CONTENTGOORUOID)+SEPERATOR+entry.getKey().toString(), averageR);
	               }
	        		if(e.get(AGGMODE)!= null && e.get(AGGMODE).toString().equalsIgnoreCase(SUM)){
	        			   updateForPostAggregate(localKey+SEPERATOR+key+SEPERATOR+entry.getKey().toString(), eventMap.get(GOORUID), 1L);
	        			   long sumOf = this.iterateAndFindSum(localKey+SEPERATOR+key+SEPERATOR+entry.getKey().toString());
		                   this.updateRealTimeAggregator(localKey,key+SEPERATOR+entry.getKey().toString(), sumOf);
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
		        			 generateCounter(localKey,key+SEPERATOR+entry.getKey(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
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
	    						generateCounter(localKey ,key+SEPERATOR+option,e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
	        					updatePostAggregator(localKey,key+SEPERATOR+option);
	    					}
	    				}else{
	    					generateCounter(localKey ,key+SEPERATOR+option,e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
        					updatePostAggregator(localKey,key+SEPERATOR+option);
	    				}
        				
        				if(!eventMap.get(QUESTIONTYPE).equalsIgnoreCase(OE) && !answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())){	    					
        					generateCounter(localKey ,key+SEPERATOR+answerStatus,1L,m);
        				}
					}
	        		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()) && e.get(AGGMODE) != null){
		        		updateForPostAggregate(localKey,key+SEPERATOR+eventMap.get(GOORUID)+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
		        		updateForPostAggregate(localKey+SEPERATOR+key,eventMap.get(GOORUID)+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
		        		updateAggregator(localKey,key+SEPERATOR+entry.getKey().toString(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
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

    public void generateCounter(String key,String columnName, long count ,MutationBatch m) {
        m.withRow(realTimeCounter, key)
        .incrementCounterColumn(columnName, count);
    }
    
    public void generateAggregator(String key,String columnName, long count,MutationBatch m ) {
        m.withRow(realTimeAggregator, key)
        .putColumnIfNotNull(columnName, count);
    }
        
    public void updateAggregator(String key,String columnName, long count ) {

    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        m.withRow(realTimeAggregator, key)
        .putColumnIfNotNull(columnName, count);
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
		if (result != null && !result.isEmpty() && result.getColumnByName(metric) != null) {
			count = result.getColumnByName(metric).getLongValue();
    	}
    	return (count);
	}

	public void realTimeAggregator(String keyValue,Map<String,String> eventMap) throws JSONException{

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		String resourceType = eventMap.get(RESOURCETYPE);
		
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
			m.withRow(realTimeAggregator, keyValue)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+ SEPERATOR+FEEDBACK,eventMap.get(TEXT),null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+FEEDBACKPROVIDER,eventMap.get(PROVIDER),null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+TIMESTAMP,eventMap.get(STARTTIME),null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+ACTIVE,eventMap.get(ACTIVE),null)
			;
		}
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			long scoreInPercentage = 0L;
			long score =  0L;
			String collectionStatus = "in-progress";
			if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){
				collectionStatus = "completed";
				 score = eventMap.get(SCORE) != null ? Long.parseLong(eventMap.get(SCORE).toString()) : 0L; 
				if(questionCountInQuiz != 0L){
					scoreInPercentage = ((score * 100/questionCountInQuiz));
				}
			}
			m.withRow(realTimeAggregator, keyValue)
			.putColumnIfNotNull(COLLECTION+ SEPERATOR+GOORUOID,eventMap.get(CONTENTGOORUOID),null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+"completion_progress",collectionStatus,null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+QUESTION_COUNT,questionCountInQuiz,null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE_IN_PERCENTAGE,scoreInPercentage,null)
			.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE,score,null)
			;
		}
		m.withRow(realTimeAggregator, keyValue)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+GOORUOID,eventMap.get(CONTENTGOORUOID),null)
		.putColumnIfNotNull(USERID,eventMap.get(GOORUID),null)
		.putColumnIfNotNull(CLASSPAGEID,eventMap.get(CLASSPAGEGOORUOID),null);
		;

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
			long totalTimeSpent = this.getCounterLongValue(keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName());
			long views = this.getCounterLongValue(keyValue, eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName());			
			if(views != 0L){
				m.withRow(realTimeAggregator, keyValue)
				.putColumnIfNotNull(eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.TS.getName(),totalTimeSpent,null)
				.putColumnIfNotNull(eventMap.get(PARENTGOORUOID)+SEPERATOR+LoaderConstants.AVGTS.getName(),(totalTimeSpent/views),null)
				;
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
    					    					
						
						
						
				      m.withRow(realTimeAggregator, keyValue)
				                .putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + SEPERATOR+TYPE ,eventMap.get(QUESTIONTYPE),null)
				      			
				      			
				      ;
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
		
		
		m.withRow(realTimeAggregator, keyValue)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + SEPERATOR+CHOICE,textValue,null)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE,scoreL,null)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + SEPERATOR+OPTIONS,options,null)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + SEPERATOR+STATUS,Long.valueOf(attemptStatus),null)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + SEPERATOR+ANSWER_OBECT,answerObject,null)
		.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID) + SEPERATOR+CHOICE,firstChoosenAns,null)
		;
		return m;								
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
	
	public ColumnList<String> getAllCounterColumns(String Key){
		
		ColumnList<String> stagedRecords = null;
    	try {
    		stagedRecords = getKeyspace().prepareQuery(realTimeCounter).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 .getKey(Key)
					 .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		return stagedRecords;
	}	
	
public ColumnList<String> getAllAggregatorColumns(String Key){
		
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
		ColumnList<String>  result = null;
		Long score = 0L;
    	try {
    		 result = getKeyspace().prepareQuery(realTimeAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		
		if (result != null && !result.isEmpty() && result.getColumnByName(columnName) != null) {
			score = result.getColumnByName(columnName).getLongValue();
    	}
    	return score;
		
	}

	public long getQuestionCount(Map<String,String> eventMap) {
		String contentGooruOId = eventMap.get(CONTENTGOORUOID);
		ColumnList<String> questionLists = null;
		long totalQuestion = 0L;
		long oeQuestion = 0L;
		long updatedQuestionCount = 0L;
			try {
				questionLists = getKeyspace().prepareQuery(questionCount)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(contentGooruOId).execute()
					.getResult();
			} catch (ConnectionException e) {			
				logger.info("Error while retieveing data : {}" ,e);
			}
			
			if((questionLists != null) && (!questionLists.isEmpty())){
					totalQuestion =  questionLists.getColumnByName("questionCount").getLongValue();
					oeQuestion =   questionLists.getColumnByName("oeCount").getLongValue();
					updatedQuestionCount = totalQuestion - oeQuestion;
			}
    	return updatedQuestionCount;
	}
	
	private boolean isRowAvailable(String key,String  columnName,String currentSession){
		ColumnList<String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(microAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		String storedSession = result.getStringValue(columnName, null);
		if (storedSession != null && !storedSession.equalsIgnoreCase(currentSession)) {
			return true;
    	}		
		return false;
		
	}
	
	private List<String> getClassPages(Map<String,String> eventMap){
    	List<String> classPages = new ArrayList<String>();
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(PARENTGOORUOID) == null){
    		List<String> parents = collectionItem.getParentId(eventMap.get(CONTENTGOORUOID));
    		if(!parents.isEmpty()){    			
    			classPages = this.getClassPagesFromItems(classPages);
    		}
    	}else if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
    		classPages.add(eventMap.get(PARENTGOORUOID));
    	}
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.get(PARENTGOORUOID) != null){
    		if(eventMap.get(CLASSPAGEGOORUOID) == null){
	    		ColumnList<String> eventDetail = eventDetailDao.readEventDetail(eventMap.get(PARENTEVENTID));
		    	if(eventDetail != null && eventDetail.size() > 0){
		    		if(eventDetail.getStringValue(EVENT_NAME, null) != null && (eventDetail.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CLPV1.getName())){
		    			classPages.add(eventDetail.getStringValue(CONTENT_GOORU_OID, null));
		    		}		    		
		    		if(eventDetail.getStringValue(EVENT_NAME, null) != null &&  (eventDetail.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			    		if(eventDetail.getStringValue(PARENT_GOORU_OID, null) == null || eventDetail.getStringValue(PARENT_GOORU_OID, null).isEmpty()){
			    			List<String> parents = collectionItem.getParentId(eventDetail.getStringValue(CONTENT_GOORU_OID, null));
			    			if(!parents.isEmpty()){    			
			        			classPages = this.getClassPagesFromItems(classPages);
			        		}
			    		}else{
			    			classPages.add(eventDetail.getStringValue(PARENT_GOORU_OID, null));
			    		}
		    		}
		    	}
	    	}
    	}
	    	if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()) && eventMap.get(CLASSPAGEGOORUOID) == null)){
	    		if(eventMap.get(CLASSPAGEGOORUOID) == null){
	            ColumnList<String> R = eventDetailDao.readEventDetail(eventMap.get(PARENTEVENTID));
	            if(R != null && R.size() > 0){
	            	String parentEventId = R.getStringValue(PARENT_EVENT_ID, null);
	            	if(parentEventId != null ){
	            		ColumnList<String> C = eventDetailDao.readEventDetail(parentEventId);
	            		if(C.getStringValue(EVENT_NAME, null) != null && (C.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CLPV1.getName())){
			    			classPages.add(C.getStringValue(CONTENT_GOORU_OID, null));
			    		}
			    		if(C.getStringValue(EVENT_NAME, null) != null &&  (C.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				    		if(C.getStringValue(PARENT_GOORU_OID, null) == null || C.getStringValue(PARENT_GOORU_OID, null).isEmpty()){
				    			List<String> parents = collectionItem.getParentId(C.getStringValue(CONTENT_GOORU_OID, null));
				    			if(!parents.isEmpty()){    			
				        			classPages = this.getClassPagesFromItems(classPages);
				        		}
				    		}else{
				    			classPages.add(C.getStringValue(PARENT_GOORU_OID, null));
				    		}
			    		}
	            	}
	            }
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

	public void addSession(String rowKey,String columnName,String value){

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

	public boolean isUserAlreadyAnswered(String key,String columnPrefix){
		ColumnList<String> counterColumns = this.getAllCounterColumns(key);
		boolean status= false;
		
		/*	long correctCount = counterColumns.getColumnByName(columnPrefix+SEPERATOR+LoaderConstants.CORRECT.getName()) != null ? counterColumns.getLongValue(columnPrefix+SEPERATOR+LoaderConstants.CORRECT.getName(), null) : 0L;
			long inCorrectCount = counterColumns.getColumnByName(columnPrefix+SEPERATOR+LoaderConstants.INCORRECT.getName()) != null ? counterColumns.getLongValue(columnPrefix+SEPERATOR+LoaderConstants.INCORRECT.getName(), null) : 0L;
			
			if(correctCount > 0L || inCorrectCount > 0L){
					status = true;
			}*/
		
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
					boolean isOwner = classpage.getClassPageOwnerInfo(eventMap.get(GOORUID),classPage);
					boolean isStudent = classpage.isUserPartOfClass(eventMap.get(GOORUID),classPage);
					if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID));
						keysList.add(ALLSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					}
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				}
			}
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID));
				keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				keysList.add(FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
			
			
		}

		this.completeMigration(eventMap, keysList);
	}
	
	public void completeMigration(Map<String,String> eventMap,List<String> keysList){
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if(keysList != null && keysList.size() > 0 ){
			for(String keyValue : keysList){
				if(eventMap.get(RESOURCETYPE) != null && eventMap.get(RESOURCETYPE).equalsIgnoreCase(QUESTION) && eventMap.get(TYPE).equalsIgnoreCase(STOP)){
					
					int[] attemptTrySequence = TypeConverter.stringToIntArray(eventMap.get(ATTMPTTRYSEQ)) ;
    				int[] attempStatus = TypeConverter.stringToIntArray(eventMap.get(ATTMPTSTATUS)) ;
    				String answerStatus = null;
					long score = 0L;
					if(attempStatus[0] == 1){
						score = 1L;
						answerStatus = LoaderConstants.CORRECT.getName();
					}else if(attempStatus[0] == 0){
						score = 0L;
						answerStatus = LoaderConstants.INCORRECT.getName();
					}
					int Status = attempStatus[0];
					
					logger.info("Status : {} ",Status);
					
					m.withRow(realTimeAggregator, keyValue)
					.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+STATUS, Long.valueOf(Status))
					.putColumnIfNotNull(eventMap.get(CONTENTGOORUOID)+SEPERATOR+SCORE, score)
					;
				}
			}
		}
	 	/*try{
         	m.execute();
         } catch (ConnectionException e) {
         	logger.info("Error while inserting to cassandra - JSON - ", e);
         }*/
	}
}
