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

import org.apache.cassandra.utils.ExpiringMap;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.TypeConverter;
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
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

@Async
public class MicroAggregatorDAOmpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO,Constants {
	
    private static final Logger logger = LoggerFactory.getLogger(MicroAggregatorDAOmpl.class);
    
    private CassandraConnectionProvider connectionProvider;
    
    private SimpleDateFormat secondsDateFormatter;
    
    private long questionCountInQuiz = 0L;
    
    private BaseCassandraRepoImpl baseCassandraDao;

    private int sleepTime = 1000;
    public MicroAggregatorDAOmpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        this.baseCassandraDao = new BaseCassandraRepoImpl(this.connectionProvider);
        this.secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
    }
    
    
    public void callCounters(final Map<String,String> eventMap) throws JSONException, ParseException {
    	
	    final Thread counterThread = new Thread(new Runnable() {
	    	@Override
	    	public void run(){
	    	String contentGooruOId = "";
			String eventName = "";
			String gooruUId = "";
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
	          }
	         });
    	counterThread.setDaemon(true);
    	counterThread.start();
    }
    
    @Async
    public void realTimeMetrics(final Map<String,String> eventMap,final String aggregatorJson) throws Exception{
    	
    final Thread realTimeThread = new Thread(new Runnable() {
	  	@Override
	  	public void run(){
	  	try{
	    	
	    	List<String> pathWays = getPathWaysFromCollection(eventMap);
	    	String key = eventMap.get(CONTENTGOORUOID);
			List<String> keysList = new ArrayList<String>();
			
			boolean isStudent = false;
			/*Maintain session - Start */
			
			if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
		        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
		        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
		        	/*isStudent = classpage.isUserPartOfClass(eventMap.get(GOORUID),eventMap.get(PARENTGOORUOID));
		        	if(isStudent){*/
		        	if(pathWays != null && pathWays.size() > 0){
						for(String pathWay : pathWays){
							String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
						}
					}
		        	//}
		        }
				baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
			}
			
			if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) ){
				Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
		        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
		        
		        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
		        	if(pathWays != null && pathWays.size() > 0){
						for(String pathWay : pathWays){
							String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
						}
		        	}
		        }
				baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
			}
			
			if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) ){
				Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
		        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
		        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
		        
		        	if(pathWays != null && pathWays.size() > 0){
						for(String pathWay : pathWays){
							String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
						}
		        	}
		        }
				baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
			}
			/*Maintain session - END */
				
			if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				questionCountInQuiz = getQuestionCount(eventMap);
				if(pathWays != null && pathWays.size() > 0){
					for(String pathWay : pathWays){
						
						String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
						
						boolean isOwner = true;
						
						isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);

						logger.info("isOwner : {}",isOwner);
						
						isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
	
						int retryCount = 1;
							
						while(!isStudent && retryCount < 3) {
					        	Thread.sleep(500);
					        	isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
					        	logger.info("retrying to check if a student 1 : {}",retryCount);
					            retryCount++;
						}
						
						logger.info("isStudent : {}",isStudent);
						
						eventMap.put(CLASSPAGEGOORUOID, classUid);
						eventMap.put(PATHWAYGOORUOID, pathWay);
						
						if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key);
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						  keysList.add(ALLSESSION+classUid+SEPERATOR+key);
						  keysList.add(ALLSESSION+classUid+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						}
						
						keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						
						logger.info("Recent Key : {} ",eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
						
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classUid+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
						
						if(!isRowAvailable(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
							keysList.add(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
							  keysList.add(FIRSTSESSION+classUid+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
							  baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classUid+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
						}
						
					}
				}
					keysList.add(ALLSESSION+key);
					keysList.add(ALLSESSION+key+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(eventMap.get(SESSION)+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					if(!isRowAvailable(FIRSTSESSION+key, eventMap.get(GOORUID),eventMap.get(SESSION))){
						keysList.add(FIRSTSESSION+key+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
			}
	
			if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()))){
	
				if(pathWays != null && pathWays.size() > 0){
					for(String pathWay : pathWays){
						
						String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
						
						boolean isOwner = true;
						
						isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
						
						logger.info("isOwner : {}",isOwner);
						
						isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
							
							int retryCount = 1;
					        while (!isStudent && retryCount < 3) {
					        	Thread.sleep(500);
					        	isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
					        	logger.info("retrying to check if a student 2 : {}",retryCount);
					            retryCount++;
					        }
	
						logger.info("isStudent : {}",isStudent);
					
						if(!isOwner && isStudent){
							keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID));
							keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
							keysList.add(ALLSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID));
							keysList.add(ALLSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						}
						keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
						
						keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
						
						if(!isRowAvailable(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
							keysList.add(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
							baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
						}
						if(!isRowAvailable(FIRSTSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
							  keysList.add(FIRSTSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
							  baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
							
						}
					}
				}
					keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID));
					keysList.add(ALLSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(eventMap.get(SESSION)+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION));
					if(!isRowAvailable(FIRSTSESSION+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION))){
						keysList.add(FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION));
					}
				
				
			}
	
			if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
				if(pathWays != null && pathWays.size() > 0){
					for(String pathWay : pathWays){
						String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
						keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						
						keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						keysList.add(ALLSESSION+classUid+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						
					}
				}
			}
				if(keysList != null && keysList.size() > 0 ){
					startCounters(eventMap, aggregatorJson, keysList, key);
					postAggregatorUpdate(eventMap, aggregatorJson, keysList, key);
					startCounterAggregator(eventMap, aggregatorJson, keysList, key);
				}
			}catch(Exception e){
				logger.info("Exception while real time aggregation :"+ e);
				e.printStackTrace();
			}
			
	  	  }
    	});
    
    	realTimeThread.setDaemon(true);
    	realTimeThread.start();
    
     }

    public void realTimeMetricsMigration(Map<String,String> eventMap,String aggregatorJson) throws JSONException{

    	List<String> pathWays = this.getPathWaysFromClass(eventMap);
    	String key = eventMap.get(CONTENTGOORUOID);
		List<String> keysList = new ArrayList<String>();
		boolean isStudent = false;

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
	        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
	        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
	        	if(pathWays != null && pathWays.size() > 0){
					for(String pathWay : pathWays){
						String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid +SEPERATOR+ pathWay+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
					}
	        	}
	        }
		}
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())  ){
			Date eventDateTime = new Date(Long.parseLong(eventMap.get(STARTTIME)));
	        String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();
	        
	        if(eventMap.get(PARENTGOORUOID) != null && !eventMap.get(PARENTGOORUOID).isEmpty()){
	        	if(pathWays != null && pathWays.size() > 0){
					for(String pathWay : pathWays){
						String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), eventMap.get(SESSION), eventRowKey);
					}
	        	}
	        }
		}
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			questionCountInQuiz = this.getQuestionCount(eventMap);
			if(pathWays != null && pathWays.size() > 0){
				for(String pathWay : pathWays){
					
					String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
					
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
					
					logger.info("isOwner : {}",isOwner);
					
					isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
					
					logger.info("isStudent : {}",isStudent);
					
					eventMap.put(CLASSPAGEGOORUOID, classUid);
					eventMap.put(PATHWAYGOORUOID, pathWay);
					
					if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key);
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					}
					
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));

					logger.info("Recent Key : {} ",eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
					
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					
					if(!this.isRowAvailable(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key, eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
						keysList.add(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
					
				}
			}
		}

		if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()))){

			if(pathWays != null && pathWays.size() > 0){
				for(String pathWay : pathWays){
					
					String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
					
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
					
					isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap.get(GOORUID),classUid,0);
					
					if(!isOwner && isStudent){
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID));
						
						keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					}
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					
					baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),RECENTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID), eventMap.get(SESSION));
					if(!this.isRowAvailable(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID), eventMap.get(GOORUID),eventMap.get(SESSION)) && !isOwner && isStudent){
						keysList.add(FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
						baseCassandraDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(),FIRSTSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+key, eventMap.get(GOORUID), eventMap.get(SESSION));
					}
				}
			}			
		}

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RUFB.getName())){
			if(pathWays != null && pathWays.size() > 0){
				for(String pathWay : pathWays){
					String classUid = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), pathWay,0);
					keysList.add(eventMap.get(SESSION)+SEPERATOR+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
					keysList.add(ALLSESSION+classUid+SEPERATOR+pathWay+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				}
			}
		}
		if(keysList != null && keysList.size() > 0 ){
			this.startCountersMig2(eventMap, aggregatorJson, keysList, key);
			//this.startCountersMig(eventMap, aggregatorJson, keysList, key);
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
	        		//if(!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE)) &&!(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && eventMap.get(TYPE).equalsIgnoreCase(STOP)) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
	        		if(!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE)) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
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
	        	if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){
	        		try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					String collectionStatus = "completed";
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
				} else if((eventMap.get(PATHWAYGOORUOID) != null) && (!eventMap.get(PATHWAYGOORUOID).isEmpty())) {
					sessionKey = "RS"+SEPERATOR+eventMap.get(PATHWAYGOORUOID)+SEPERATOR+eventMap.get(PARENTGOORUOID);
				}else {
					sessionKey = "RS"+SEPERATOR+eventMap.get(PARENTGOORUOID);
				}
				Column<String> session = baseCassandraDao.readWithKeyColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), sessionKey,eventMap.get(GOORUID),0);
				sessionId = session != null ? session.getStringValue() : null;
				
				if((sessionId != null) && (!sessionId.isEmpty())) {
					newKey = keyValue.replaceFirst("AS~", sessionId+SEPERATOR);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+ SEPERATOR+FEEDBACK, eventMap.containsKey(TEXT) ? eventMap.get(TEXT) : null, m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+FEEDBACKPROVIDER,eventMap.containsKey(PROVIDER) ? eventMap.get(PROVIDER) : null, m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+TIMESTAMP,Long.valueOf(eventMap.get(STARTTIME)), m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENTGOORUOID)+SEPERATOR+ACTIVE,eventMap.containsKey(ACTIVE) ? eventMap.get(ACTIVE) : null, m);
					
				}
			}
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+ SEPERATOR+FEEDBACK, eventMap.containsKey(TEXT) ? eventMap.get(TEXT) : null, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+FEEDBACKPROVIDER,eventMap.containsKey(PROVIDER) ? eventMap.get(PROVIDER) : null, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+TIMESTAMP,Long.valueOf(eventMap.get(STARTTIME)), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+ACTIVE,eventMap.containsKey(ACTIVE) ? eventMap.get(ACTIVE) : null, m);
		}
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENTGOORUOID)+SEPERATOR+GOORUOID,eventMap.get(CONTENTGOORUOID), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, USERID,eventMap.get(GOORUID), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, CLASSPAGEID,eventMap.get(CLASSPAGEGOORUOID), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, PATHWAYID,eventMap.get(PATHWAYGOORUOID), m);
			
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

	private List<String> getPathwayFromItems(List<String> parentIds){
		List<String> pathwayIds = new ArrayList<String>();
		for(String pathwayId : parentIds){
			String type = null;

			ColumnList<String> resourcesDetail = baseCassandraDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~" + pathwayId,0);
			
			type = resourcesDetail.getStringValue("type_name", null);

			if(type != null && type.equalsIgnoreCase(LoaderConstants.PATHWAY.getName())){
				pathwayIds.add(pathwayId);
			}
		}
		return pathwayIds;
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
	
	public List<String> getPathWaysFromCollection(Map<String,String> eventMap){
    	List<String> classPages = new ArrayList<String>();
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(PARENTGOORUOID) == null){
    		List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(CONTENTGOORUOID),0);
    		if(!parents.isEmpty()){    			
    			classPages = this.getPathwayFromItems(parents);
    		}
    	}else if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.get(PARENTGOORUOID) != null){

    		String type = null;
    		
    		ColumnList<String> resourcesDetail = baseCassandraDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~" + eventMap.get(PARENTGOORUOID),0);
			
			type = resourcesDetail.getStringValue("type_name", null);
			
			if(type != null && type.equalsIgnoreCase(LoaderConstants.PATHWAY.getName())){
				classPages.add(eventMap.get(PARENTGOORUOID));
			}else if(type != null && type.equalsIgnoreCase(LoaderConstants.CLASSPAGE.getName())){
				List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(CONTENTGOORUOID),0);
	    		if(!parents.isEmpty()){    			
	    			classPages = this.getPathwayFromItems(parents);
	    		}
			}
    	}
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.get(PARENTGOORUOID) != null){
	    		ColumnList<String> eventDetail = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventMap.get(PARENTEVENTID),0);
		    	if(eventDetail != null && eventDetail.size() > 0){
		    		if(eventDetail.getStringValue(EVENT_NAME, null) != null &&  (eventDetail.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			    		if(eventDetail.getStringValue(PARENT_GOORU_OID, null) == null || eventDetail.getStringValue(PARENT_GOORU_OID, null).isEmpty()){
			    			List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventDetail.getStringValue(CONTENT_GOORU_OID, null),0);
			    			if(!parents.isEmpty()){    			
			        			classPages = this.getPathwayFromItems(parents);
			        		}
			    		}else{
			        		String type = null;
			        		
			        		ColumnList<String> resourcesDetail = baseCassandraDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~" + eventDetail.getStringValue(PARENT_GOORU_OID, null),0);
			        		type = resourcesDetail.getStringValue("type_name", null);
			    			
			    			if(type != null && type.equalsIgnoreCase(LoaderConstants.PATHWAY.getName())){
			    				classPages.add(eventDetail.getStringValue(PARENT_GOORU_OID, null));
			    			}else if(type != null && type.equalsIgnoreCase(LoaderConstants.CLASSPAGE.getName())){
			    				
			    				List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventDetail.getStringValue(CONTENT_GOORU_OID, null),0);
				    			if(!parents.isEmpty()){    			
				        			classPages = this.getPathwayFromItems(parents);
				        		}
			    			}
			    		}
		    	}
	    	}else{
	    		List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(PARENTGOORUOID),0);
    			if(!parents.isEmpty()){    			
        			classPages = this.getPathwayFromItems(parents);
        		}
	    	}
    	}
	    	if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()))){
	            ColumnList<String> R = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventMap.get(PARENTEVENTID),0);
	            if(R != null && R.size() > 0){
	            	String parentEventId = R.getStringValue(PARENT_EVENT_ID, null);
	            	if(parentEventId != null ){
	            		ColumnList<String> C = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),parentEventId,0);
			    		if(C.getStringValue(EVENT_NAME, null) != null &&  (C.getStringValue(EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				    		if(C.getStringValue(PARENT_GOORU_OID, null) == null || C.getStringValue(PARENT_GOORU_OID, null).isEmpty()){
				    			List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),C.getStringValue(CONTENT_GOORU_OID, null),0);
				    			if(!parents.isEmpty()){    			
				        			classPages = this.getPathwayFromItems(parents);
				        		}
				    		}else{

				        		String type = null;
				        		
				        			ColumnList<String> resourcesDetail = baseCassandraDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~" + C.getStringValue(PARENT_GOORU_OID, null),0);
				    			
				    				type = resourcesDetail.getStringValue("type_name", null);
				    				
				    			if(type != null && type.equalsIgnoreCase(LoaderConstants.PATHWAY.getName())){
				    				classPages.add(C.getStringValue(PARENT_GOORU_OID, null));
				    			}else{
				    				List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),C.getStringValue(CONTENT_GOORU_OID, null),0);
					    			if(!parents.isEmpty()){    			
					        			classPages = this.getPathwayFromItems(parents);
					        		}
				    			}
				    		
				    		}
			    		}
	            	}
	        }
    	}
	    
	    	return classPages;
	}

	public List<String> getPathWaysFromClass(Map<String,String> eventMap){

		logger.info("pathway migration from class.. ");
		
		logger.info("Event name : " + eventMap.get(EVENTNAME) + "content_gooru_id " + eventMap.get(CONTENTGOORUOID));
		
		List<String> classPages = new ArrayList<String>();
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
    		List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(CONTENTGOORUOID),0);
    		if(!parents.isEmpty()){    			
    			classPages = this.getPathwayFromItems(parents);
    		}
    	}
    	if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.get(PARENTGOORUOID) != null){

    		List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap.get(PARENTGOORUOID),0);
				if(!parents.isEmpty()){    			
					classPages = this.getPathwayFromItems(parents);
			    }
			    			
    	}
	    	if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName()))){
	            ColumnList<String> R = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventMap.get(PARENTEVENTID),0);
	            if(R != null && R.size() > 0){
				    	List<String> parents = baseCassandraDao.getParentIds(ColumnFamily.COLLECTIONITEM.getColumnFamily(),R.getStringValue(PARENT_GOORU_OID, null),0);
				    	if(!parents.isEmpty()){    			
				        	classPages = this.getPathwayFromItems(parents);
				        }
				  }			    		
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
	
	public void updateRawData(final Map<String,String> eventMap){
		 final Thread updateRawThread = new Thread(new Runnable() {
		    	@Override
		    	public void run(){
					if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CCV1.getName())){
						baseCassandraDao.saveStringValue(ColumnFamily.COLLECTION.getColumnFamily(), eventMap.get(CONTENTID), CONTENT_ID, eventMap.get(CONTENTID));
						baseCassandraDao.updateCollectionItem(ColumnFamily.COLLECTIONITEM.getColumnFamily(),eventMap);
					}
					if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CLUAV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CLPCV1.getName())){
						baseCassandraDao.updateClasspage(ColumnFamily.CLASSPAGE.getColumnFamily(),eventMap);
					}
		    	}
		 });
		 updateRawThread.setDaemon(true);
		 updateRawThread.start();
	}

	//to be removed 
	public void startCountersMig2(Map<String,String> eventMap,String aggregatorJson,List<String> keysList,String key) throws JSONException{    	
    	JSONObject j = new JSONObject(aggregatorJson);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
    	
    	for (Entry entry : entrySet) {
        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
	        for(String localKey : keysList){
	        
	        		if(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && (eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()))){

	        			ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,0);
	        			
	        			long views = counterColumns.getColumnByName(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName()) != null ? counterColumns.getLongValue(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName(), 0L) : 0L;
	        			
	        			if(views == 0L){
	        				baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,key+SEPERATOR+entry.getKey(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
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
	 public void startCountersMig(Map<String,String> eventMap,String aggregatorJson,List<String> keysList,String key) throws JSONException{    	
	    	JSONObject j = new JSONObject(aggregatorJson);
	    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			Map<String, Object> m1 = JSONDeserializer.deserialize(j.toString(), new TypeReference<Map<String, Object>>() {});
	    	Set<Map.Entry<String, Object>> entrySet = m1.entrySet();
	    	
	    	for (Entry entry : entrySet) {
	        	Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
	        	Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
		        for(String localKey : keysList){
		        
			        	if(e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(COUNTER)){
			        		if(!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE)) &&!(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName())) && !eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRAV1.getName())){
				        			 baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,key+SEPERATOR+entry.getKey(),e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()),m);
						}	
		        		
		        		if(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && (eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()))){

		        			ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(),localKey,0);
		        			
		        			long views = counterColumns.getColumnByName(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName()) != null ? counterColumns.getLongValue(eventMap.get(CONTENTGOORUOID)+SEPERATOR+LoaderConstants.TOTALVIEWS.getName(), 0L) : 0L;
		        			
		        			if(views == 0L){
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

}
