/*******************************************************************************
 * EventServiceImpl.java
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
package org.logger.event.web.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.ednovo.data.model.EventObjectValidator;
import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.web.controller.dto.ActionResponseDTO;
import org.logger.event.web.utils.ServerValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.validation.BindException;
import org.springframework.validation.Errors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class EventServiceImpl implements EventService, Constants {
	
	protected final Logger logger = LoggerFactory.getLogger(EventServiceImpl.class);

    protected CassandraDataLoader dataLoaderService;
    private final CassandraConnectionProvider connectionProvider;
    private EventObjectValidator eventObjectValidator;
    private BaseCassandraRepoImpl baseDao ;
    private SimpleDateFormat minuteDateFormatter;
    Calendar cal = Calendar.getInstance();
    
    public EventServiceImpl() {
        dataLoaderService = new CassandraDataLoader();
        this.connectionProvider = dataLoaderService.getConnectionProvider();
        baseDao = new BaseCassandraRepoImpl(connectionProvider);
        eventObjectValidator = new EventObjectValidator(null);
        this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
        
    }

    @Override
    public ActionResponseDTO<EventData> handleLogMessage(EventData eventData) {

        Errors errors = validateInsertEventData(eventData);

        if (!errors.hasErrors()) {
            dataLoaderService.handleLogMessage(eventData);
        }

        return new ActionResponseDTO<EventData>(eventData, errors);
    }
    

    @Override
    public AppDO verifyApiKey(String apiKey) {
        ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(), apiKey,0);
        AppDO appDO = new AppDO();
        appDO.setApiKey(apiKey);
        appDO.setAppName(apiKeyValues.getStringValue("appName", null));
        appDO.setEndPoint(apiKeyValues.getStringValue("endPoint", null));
        appDO.setDataPushingIntervalInMillsecs(apiKeyValues.getStringValue("pushIntervalMs", null));
        return appDO;
    }

    private Errors validateInsertEventData(EventData eventData) {
        final Errors errors = new BindException(eventData, "EventData");
        if (eventData == null) {
            ServerValidationUtils.rejectIfNull(errors, eventData, "eventData.all", "Fields must not be empty");
            return errors;
        }
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventData.getEventName(), "eventName", "LA001", "eventName must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventData.getEventId(), "eventId", "LA002", "eventId must not be empty");

        return errors;
    }

    private Errors validateInsertEventObject(EventObject eventObject) {
        final Errors errors = new BindException(eventObject, "EventObject");
        if (eventObject == null) {
            ServerValidationUtils.rejectIfNull(errors, eventObject, "eventData.all", "Fields must not be empty");
            return errors;
        }
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getEventName(), "eventName", "LA001", "eventName must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getEventId(), "eventId", "LA002", "eventId must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getVersion(), "version", "LA003", "version must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getUser(), "user", "LA004", "User Object must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getSession(), "session", "LA005", "Session Object must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getMetrics(), "metrics", "LA006", "Mestrics Object must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getContext(), "context", "LA007", "context Object must not be empty");
        ServerValidationUtils.rejectIfNullOrEmpty(errors, eventObject.getPayLoadObject(), "payLoadObject", "LA008", "pay load Object must not be empty");
        return errors;
    }
	@Override
	public ColumnList<String> readEventDetail(String eventKey) {
		ColumnList<String> eventColumnList = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(),eventKey,0);
		return eventColumnList;
	}

	@Override
	public Rows<String, String> readLastNevents(String apiKey,
			Integer rowsToRead) {
		Rows<String, String> eventRowList = baseDao.readIndexedColumnLastNrows(ColumnFamily.EVENTDETAIL.getColumnFamily(), "api_key", apiKey, rowsToRead,0);
		return eventRowList;
	}

	@Override
	public void updateProdViews() {
		try {
			dataLoaderService.callAPIViewCount();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public List<Map<String, Object>> readUserLastNEventsResourceIds(String userUid, String startTime, String endTime, String eventName, Integer eventsToRead){
		String activity = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;
		
		List<Map<String, Object>>  resultList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>>  valueList = new ArrayList<Map<String, Object>>();
		JsonElement jsonElement = null;
		ColumnList<String> activityJsons;
		
		if(eventName != null){
			startColumnPrefix = startTime+"~"+eventName;
			endColumnPrefix = endTime+"~"+eventName;
		} else {
			startColumnPrefix = endTime;
			endColumnPrefix = endTime;
		}
		
		activityJsons = baseDao.readColumnsWithPrefix(ColumnFamily.ACTIVITYSTREAM.getColumnFamily(),userUid, startColumnPrefix, endColumnPrefix, eventsToRead);
		if((activityJsons == null || activityJsons.isEmpty() || activityJsons.size() == 0 || activityJsons.size() < 30) && eventName == null) {
			activityJsons = baseDao.readKeyLastNColumns(ColumnFamily.ACTIVITYSTREAM.getColumnFamily(),userUid, eventsToRead,0);
		}	
		for (Column<String> activityJson : activityJsons) {
			Map<String, Object> valueMap = new HashMap<String, Object>();
			activity = activityJson.getStringValue();
			if(!activity.isEmpty()){
				try {
					//validate JSON
					jsonElement = new JsonParser().parse(activity.toString());
					JsonObject eventObj = jsonElement.getAsJsonObject();

					if(eventObj.get("content_gooru_oid") != null){
						valueMap.put("resourceId", eventObj.get("content_gooru_oid").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("parent_gooru_oid") != null){
						valueMap.put("parentId", eventObj.get("parent_gooru_oid").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("event_name") != null){
						valueMap.put("eventName", eventObj.get("event_name").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("user_uid") != null){
						valueMap.put("userUid", eventObj.get("user_uid").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("username") != null){
						valueMap.put("username", eventObj.get("username").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("score") != null){
						valueMap.put("score", eventObj.get("score").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("session_id") != null){
						valueMap.put("sessionId", eventObj.get("session_id").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("first_attempt_status") != null){
						valueMap.put("firstAttemptStatus", eventObj.get("first_attempt_status").toString().replaceAll("\"", ""));
					}
					if(eventObj.get("answer_status") != null){
						valueMap.put("answerStatus", eventObj.get("answer_status").toString().replaceAll("\"", ""));
					}
					/*if(eventObj.get("date_time") != null){
						valueMap.put("dateTime", eventObj.get("date_time").toString().replaceAll("\"", ""));
					}*/
				} catch (JsonParseException e) {
				    // Invalid.
					logger.error("OOPS! Invalid JSON", e);
				}		
			}
			valueList.add(valueMap);
		}		
		resultList.addAll(valueList);
		return resultList;
	}

	@Override
	@Async
	public ActionResponseDTO<EventObject> handleEventObjectMessage(
			EventObject eventObject) throws JSONException, ConnectionException, IOException, GeoIp2Exception {
		
        Errors errors = validateInsertEventObject(eventObject);        
        if (!errors.hasErrors()) {
        		eventObjectValidator.validateEventObject(eventObject);
				dataLoaderService.handleEventObjectMessage(eventObject);
        }
        return new ActionResponseDTO<EventObject>(eventObject, errors);
	}
	
	@Async
	public void watchSession(){
		dataLoaderService.watchSession();
	}
	
	@Async
	public void executeForEveryMinute(String startTime,String endTime){
		dataLoaderService.executeForEveryMinute(startTime, endTime);
	}
	
	public boolean createEvent(String eventName,String apiKey){
		return dataLoaderService.createEvent(eventName,apiKey);
	}
	
	public boolean validateSchedular(String ipAddress){
		
		return dataLoaderService.validateSchedular(ipAddress);
	}
	public void postMigration(String start,String end,String param){
			dataLoaderService.postMigration(start, end, param);
	}
	public void postStatMigration(String start,String end,String param){
		dataLoaderService.postStatMigration(start, end, param);
	}
	public void catalogMigration(String start,String end,String param){
		dataLoaderService.catalogMigration(start, end, param);
	}

	@Override
	public void balanceStatDataUpdate() {
		dataLoaderService.balanceStatDataUpdate();
	}
	public void clearCacher(){
		dataLoaderService.clearCache();
	}

	@Override
	public void migrateLiveDashBoard() {
		dataLoaderService.migrateLiveDashBoard();
	}

	@Override
	public void migrateVTLiveDashBoard() {
		dataLoaderService.migrateViewsTimespendLiveDashBoard();
	}
	
	
	@Override
	public void eventMigration() {		
		String lastUpadatedTime = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~last~updated", "constant_value",0).getStringValue();
        String currentTime = minuteDateFormatter.format(new Date()).toString();
        String status = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~status", "constant_value",0).getStringValue();
        logger.info("lastUpadatedTime : " + lastUpadatedTime + " - currentTime" + currentTime);
        if(status.equalsIgnoreCase("completed") && !lastUpadatedTime.equalsIgnoreCase(currentTime) && Long.parseLong(lastUpadatedTime) <= Long.parseLong(currentTime)){
        	try{
				dataLoaderService.eventMigration(lastUpadatedTime, currentTime, null,true);
			}catch (ParseException e) {
			 e.printStackTrace();
			}
        }else{
        	logger.info("Waitingg...");
        	String lastCheckedCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~checked~count", "constant_value",0).getStringValue();
            String lastMaxCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~max~count", "constant_value",0).getStringValue();

            if(Integer.valueOf(lastCheckedCount) < Integer.valueOf(lastMaxCount)){
                    baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~checked~count", "constant_value", ""+ (Integer.valueOf(lastCheckedCount) + 1));
            }else{
                    baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "activity~migration~status", "constant_value","completed");
            }

        }
		
	}

	@Override
	public void viewsTsMigration(String ids) {
		dataLoaderService.migrateViewCountTs(ids);		
	}

	@Override
	public void migrateContentAndIndex(String indexName, String indexType, String lookUpField, String lookUpValue, int limit, boolean migrate) {
		dataLoaderService.migrateContentAndIndex(indexName, indexType, lookUpField, lookUpValue, limit, migrate);
	}
	
	@Override
 	public void assementScoreCalculator() {
		dataLoaderService.assementScoreCalculator();
	}
	
	@Override
	public void index(String ids,String indexType){
		try {
			if(indexType.equalsIgnoreCase("resource")){
				dataLoaderService.indexResource(ids);
			}
			if(indexType.equalsIgnoreCase("user")){
				dataLoaderService.indexUser(ids);
			}
			if(indexType.equalsIgnoreCase("event")){
				dataLoaderService.indexEvent(ids);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Async
	@Override
	public void migrateEventAndIndex() {
		Long numberOfJobsRunning = 0L;
		Long maxJobs = 0L;
		String indexName = null;
		String indexType = null;
		String lookUpField = null;
		Integer limit = 1000;
		Boolean isMigrate = true;
		
		ColumnList<String> columns = baseDao.readWithKey(AWS_CASSANDRA_VERSION, ColumnFamily.CONFIGSETTINGS.getColumnFamily(), EVENT_MIGRATION_AND_INDEX, DEFAULT_RETRY_COUNT);
		numberOfJobsRunning = Long.valueOf(columns.getStringValue(DEFAULTCOLUMN, ZERO));
		maxJobs = Long.valueOf(columns.getStringValue(MAX_JOBS, ZERO));
		indexName = columns.getStringValue(INDEX_NAME, null);
		indexType = columns.getStringValue(INDEX_TYPE, null);
		lookUpField = columns.getStringValue(LOOK_UP_FIELD, null);
		limit = Integer.valueOf(columns.getStringValue(LIMIT_NAME, DEFAULT_EVENT_LIMIT.toString()));
		isMigrate = Boolean.valueOf(columns.getStringValue(MIGRATE, TRUE));
		String gooruUid = null;
		if(numberOfJobsRunning < maxJobs && indexName != null && indexType != null && lookUpField != null) {
			try {
				long startTime = new Date().getTime();
				arithmeticOperations(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), EVENT_MIGRATION_AND_INDEX, DEFAULTCOLUMN, ADD);
				gooruUid = baseDao.readIndexedColumnLastNrows(AWS_CASSANDRA_VERSION, ColumnFamily.USER_EVENT_INDEX_QUEUE.getColumnFamily(), FIELD_EVENT_STATUS, READY, 1, DEFAULT_RETRY_COUNT).getRowByIndex(0).getKey();
				baseDao.saveStringValue(AWS_CASSANDRA_VERSION, ColumnFamily.USER_EVENT_INDEX_QUEUE.getColumnFamily(), gooruUid, FIELD_EVENT_STATUS, INPROGRESS);
				this.migrateContentAndIndex(indexName, indexType, lookUpField, gooruUid, limit.intValue(), isMigrate);
				baseDao.saveStringValue(AWS_CASSANDRA_VERSION, ColumnFamily.USER_EVENT_INDEX_QUEUE.getColumnFamily(), gooruUid, FIELD_EVENT_STATUS, COMPLETED);
				long endTime = new Date().getTime();
				baseDao.saveLongValue(AWS_CASSANDRA_VERSION, ColumnFamily.USER_EVENT_INDEX_QUEUE.getColumnFamily(), gooruUid, TIME_TAKEN, (endTime - startTime));
				arithmeticOperations(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), EVENT_MIGRATION_AND_INDEX, DEFAULTCOLUMN, SUB);
			}
			catch(Exception e) {
				logger.error("Error in event migrate and indexing job... {}", e.getMessage());
				if(gooruUid != null) {
					baseDao.saveStringValue(AWS_CASSANDRA_VERSION, ColumnFamily.USER_EVENT_INDEX_QUEUE.getColumnFamily(), gooruUid, FIELD_EVENT_STATUS, ERROR);
				}
				arithmeticOperations(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), EVENT_MIGRATION_AND_INDEX, DEFAULTCOLUMN, SUB);
			}
		} 
		else {
			logger.info("Migration Event And Index Job - Reached Max Limit: {}", maxJobs);
		}
	}
	
	private synchronized void arithmeticOperations(String cfName, String key, String columnName, String operation) {
		Column<String> column = baseDao.readWithKeyColumn(AWS_CASSANDRA_VERSION, cfName, key, columnName, DEFAULT_RETRY_COUNT);
		if(column != null) {
			if(operation.equalsIgnoreCase(ADD)) {
				baseDao.saveStringValue(AWS_CASSANDRA_VERSION, cfName, key, columnName, String.valueOf(Long.valueOf(column.getStringValue()) + 1));
			} else if(operation.equalsIgnoreCase(SUB)) {
				baseDao.saveStringValue(AWS_CASSANDRA_VERSION, cfName, key, columnName, String.valueOf(Long.valueOf(column.getStringValue()) - 1));
			} else {
				logger.error("InValid Operation...");
			}
		}
		else {
			logger.error("Column {} not exist.", columnName);
		}
	}
	
	@Override
	@Async
	public void indexContent() {
		Long numberOfJobsRunning = 0L;
		Long maxJobs = 0L;
		Integer limit = 1000;
		Long indexedContent = 0L;
		Long maxContent = 0L;
		
		ColumnList<String> columns = baseDao.readWithKey(AWS_CASSANDRA_VERSION, ColumnFamily.CONFIGSETTINGS.getColumnFamily(), CONTENT_INDEXING_JOB, DEFAULT_RETRY_COUNT);
		indexedContent = Long.valueOf(columns.getStringValue(INDEXED_CONTENT, ZERO));
		maxContent = Long.valueOf(columns.getStringValue(MAX_COUNT, ZERO));
		numberOfJobsRunning = Long.valueOf(columns.getStringValue(RUNNING_JOBS, ZERO));
		maxJobs = Long.valueOf(columns.getStringValue(MAX_JOBS, ZERO));
		limit = Integer.valueOf(columns.getStringValue(LIMIT_NAME, DEFAULT_EVENT_LIMIT.toString()));
		try {
			if((indexedContent < maxContent) && (numberOfJobsRunning < maxJobs)) {
				logger.info("Started content indexing job. Job number: {}, Indexing content from : {}", numberOfJobsRunning, indexedContent);
				baseDao.saveStringValue(AWS_CASSANDRA_VERSION, ColumnFamily.CONFIGSETTINGS.getColumnFamily(), CONTENT_INDEXING_JOB, INDEXED_CONTENT, String.valueOf(indexedContent + limit));
				arithmeticOperations(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), CONTENT_INDEXING_JOB, RUNNING_JOBS, ADD);
				for(Long index = indexedContent; index <= (indexedContent + limit); index ++) {
					Rows<String, String> rows = baseDao.readIndexedColumn(ColumnFamily.DIMRESOURCE.getColumnFamily(), CONTENT_ID, index, DEFAULT_RETRY_COUNT);
					if(rows != null && !rows.isEmpty()) {
						for(Row<String, String> row : rows) {
							String gooruOid = row.getColumns().getStringValue(GOORUOID, null);
							if(gooruOid != null && gooruOid != EMPTY_STRING) {
								dataLoaderService.indexResource(gooruOid);
							}
						}
					} else {
						logger.info("Content not found: {}", index);
					}
				}
				arithmeticOperations(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), CONTENT_INDEXING_JOB, RUNNING_JOBS, SUB);
				logger.info("Completed the content indexing job. Job number: {}", numberOfJobsRunning);
			}
		}
		catch(Exception e) {
			logger.error("Error in content indexing job... {}", e.getMessage());
			arithmeticOperations(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), CONTENT_INDEXING_JOB, RUNNING_JOBS, SUB);
		}
	}
}
