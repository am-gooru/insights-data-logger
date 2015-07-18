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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.google.gson.Gson;
import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.EventData;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataLoggerCaches;
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
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class EventServiceImpl implements EventService, Constants {

	protected final Logger logger = LoggerFactory.getLogger(EventServiceImpl.class);

	protected CassandraDataLoader dataLoaderService;
	private final CassandraConnectionProvider connectionProvider;
	private BaseCassandraRepoImpl baseDao;
	private SimpleDateFormat minuteDateFormatter;
	private DataLoggerCaches loggerCache;
	private final Gson gson;
	
	public EventServiceImpl() {
		gson = new Gson();
		setLoggerCache(new DataLoggerCaches());
		dataLoaderService = new CassandraDataLoader();
		this.connectionProvider = dataLoaderService.getConnectionProvider();
		baseDao = new BaseCassandraRepoImpl(connectionProvider);
		this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		minuteDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	/**
	 * 
	 */
	@Override
	public ActionResponseDTO<EventData> handleLogMessage(EventData eventData) {

		Errors errors = validateInsertEventData(eventData);

		if (!errors.hasErrors()) {
			dataLoaderService.handleLogMessage(eventData);
		}

		return new ActionResponseDTO<EventData>(eventData, errors);
	}

	/**
	 * 
	 */

	@Override
	public AppDO verifyApiKey(String apiKey) {
		ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(), apiKey, 0);
		AppDO appDO = new AppDO();
		appDO.setApiKey(apiKey);
		appDO.setAppName(apiKeyValues.getStringValue("appName", null));
		appDO.setEndPoint(apiKeyValues.getStringValue("endPoint", null));
		appDO.setDataPushingIntervalInMillsecs(apiKeyValues.getStringValue("pushIntervalMs", null));
		return appDO;
	}

	/**
	 * 
	 * @param eventData
	 * @return
	 */
	private Errors validateInsertEventData(EventData eventData) {
		final Errors errors = new BindException(eventData, "EventData");
		if (eventData == null) {
			ServerValidationUtils.rejectIfNull(errors, eventData, "eventData.all", FIELDS+EMPTY_EXCEPTION);
			return errors;
		}
		ServerValidationUtils.rejectIfNullOrEmpty(errors, eventData.getEventName(), EVENT_NAME, "LA001", EVENT_NAME+EMPTY_EXCEPTION);
		ServerValidationUtils.rejectIfNullOrEmpty(errors, eventData.getEventId(), EVENT_ID, "LA002", EVENT_ID+EMPTY_EXCEPTION);

		return errors;
	}

	/**
	 * 
	 * @param event
	 * @return
	 */
	@Async
	private Boolean validateInsertEvent(Event event) {
		Boolean isValidEvent = true;
		if (event == null) {
			ServerValidationUtils.logErrorIfNull(isValidEvent, event, "event.all", RAW_EVENT_NULL_EXCEPTION);
		}
		String eventJson = gson.toJson(event);

		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getEventName(), EVENT_NAME, "LA001", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getEventId(), EVENT_ID, "LA002", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getVersion(), VERSION, "LA003", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getUser(), USER, "LA004", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getSession(), SESSION, "LA005", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getMetrics(), METRICS, "LA006", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getContext(), CONTEXT, "LA007", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getPayLoadObject(), PAY_LOAD, "LA008", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfZeroLongValue(isValidEvent, event.getStartTime(), START_TIME, "LA009", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfZeroLongValue(isValidEvent, event.getEndTime(), END_TIME, "LA010", eventJson, RAW_EVENT_NULL_EXCEPTION);
		if (isValidEvent) {
			try {
				JSONObject session = new JSONObject(event.getSession());
				if (event.getEventName().matches(SESSION_ACTIVITY_EVENTS)) {
					if (!session.has(SESSION_ID)
							|| (session.has(SESSION_ID) && (session.isNull(SESSION_ID) || (session.get(SESSION_ID) != null && session.getString(SESSION_ID).equalsIgnoreCase("null"))))) {
						isValidEvent = false;
						logger.error(RAW_EVENT_NULL_EXCEPTION + SESSION_ID + " : " + gson.toJson(event).toString());
					}
				}

			} catch (JSONException e) {
				isValidEvent = false;
				logger.error(RAW_EVENT_JSON_EXCEPTION + SESSION + " : " + gson.toJson(event).toString());
			}
			try {
				JSONObject context = new JSONObject(event.getContext());
				if (event.getEventName().matches(SESSION_ACTIVITY_EVENTS)) {
					if (!context.has(CONTENT_GOORU_OID)
							|| (context.has(CONTENT_GOORU_OID) && (context.isNull(CONTENT_GOORU_OID) || (context.get(CONTENT_GOORU_OID) != null && context.getString(CONTENT_GOORU_OID).equalsIgnoreCase("null"))))) {
						isValidEvent = false;
						logger.error(RAW_EVENT_NULL_EXCEPTION + CONTENT_GOORU_OID + " : " + gson.toJson(event).toString());
					}
				}
			} catch (JSONException e) {
				isValidEvent = false;
				logger.error(RAW_EVENT_JSON_EXCEPTION + CONTEXT + " : " + gson.toJson(event).toString());
			}
		}
		
		return isValidEvent;
	}

	@Override
	public ColumnList<String> readEventDetail(String eventKey) {
		ColumnList<String> eventColumnList = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventKey, 0);
		return eventColumnList;
	}

	@Override
	public Rows<String, String> readLastNevents(String apiKey, Integer rowsToRead) {
		Rows<String, String> eventRowList = baseDao.readIndexedColumnLastNrows(ColumnFamily.EVENTDETAIL.getColumnFamily(), "api_key", apiKey, rowsToRead, 0);
		return eventRowList;
	}

	@Override
	public List<Map<String, Object>> readUserLastNEventsResourceIds(String userUid, String startTime, String endTime, String eventName, Integer eventsToRead) {
		String activity = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> valueList = new ArrayList<Map<String, Object>>();
		JsonElement jsonElement = null;
		ColumnList<String> activityJsons;

		if (eventName != null) {
			startColumnPrefix = startTime + SEPERATOR + eventName;
			endColumnPrefix = endTime + SEPERATOR + eventName;
		} else {
			startColumnPrefix = endTime;
			endColumnPrefix = endTime;
		}

		activityJsons = baseDao.readColumnsWithPrefix(ColumnFamily.ACTIVITYSTREAM.getColumnFamily(), userUid, startColumnPrefix, endColumnPrefix, eventsToRead);
		if ((activityJsons == null || activityJsons.isEmpty() || activityJsons.size() == 0 || activityJsons.size() < 30) && eventName == null) {
			activityJsons = baseDao.readKeyLastNColumns(ColumnFamily.ACTIVITYSTREAM.getColumnFamily(), userUid, eventsToRead, 0);
		}
		for (Column<String> activityJson : activityJsons) {
			Map<String, Object> valueMap = new HashMap<String, Object>();
			activity = activityJson.getStringValue();
			if (!activity.isEmpty()) {
				try {
					// validate JSON
					jsonElement = new JsonParser().parse(activity);
					JsonObject eventObj = jsonElement.getAsJsonObject();

					if (eventObj.get(_CONTENT_GOORU_OID) != null) {
						valueMap.put(RESOURCE_ID, eventObj.get(_CONTENT_GOORU_OID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(_PARENT_GOORU_OID) != null) {
						valueMap.put(PARENT_ID, eventObj.get(_PARENT_GOORU_OID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(_EVENT_NAME) != null) {
						valueMap.put(EVENT_NAME, eventObj.get(_EVENT_NAME).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(_USER_UID) != null) {
						valueMap.put(USER_UID, eventObj.get(_USER_UID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(USERNAME) != null) {
						valueMap.put(USERNAME, eventObj.get(USERNAME).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(SCORE) != null) {
						valueMap.put(SCORE, eventObj.get(SCORE).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(_SESSION_ID) != null) {
						valueMap.put(SESSION_ID, eventObj.get(_SESSION_ID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(_FIRST_ATTEMPT_STATUS) != null) {
						valueMap.put(FIRST_ATTEMPT_STATUS, eventObj.get(_FIRST_ATTEMPT_STATUS).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}
					if (eventObj.get(_ANSWER_STATUS) != null) {
						valueMap.put(ANSWERSTATUS, eventObj.get(_ANSWER_STATUS).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
					}

				} catch (JsonParseException e) {
					// Invalid.
					logger.error(INVALID_JSON, e);
				}
			}
			valueList.add(valueMap);
		}
		resultList.addAll(valueList);
		return resultList;
	}

	@Override
	public void processMessage(Event event){
		Boolean isValidEvent = validateInsertEvent(event);
		if (isValidEvent) {
			dataLoaderService.processMessage(event);
		}
	}

	public void runMicroAggregation(String startTime, String endTime) {
		dataLoaderService.runMicroAggregation(startTime, endTime);
	}

	public boolean createEvent(String eventName, String apiKey) {
		return dataLoaderService.createEvent(eventName, apiKey);
	}

	public boolean validateSchedular() {
		return dataLoaderService.validateSchedular();
	}

	public void clearCache() {
		setLoggerCache(new DataLoggerCaches());
		logger.debug("after clearing cache:"+getLoggerCache().canRunScheduler);
	}

	public void indexActivity() {
		String lastUpadatedTime = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_LAST_UPDATED, DEFAULT_COLUMN, 0).getStringValue();
		String currentTime = minuteDateFormatter.format(new Date());
		logger.info("lastUpadatedTime: " + lastUpadatedTime + " - currentTime: " + currentTime);
		Date lastDate = null;
		Date currDate = null;
		String status = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_STATUS, DEFAULT_COLUMN, 0).getStringValue();
		if (status.equalsIgnoreCase(COMPLETED)) {
			// All past indexing complete. Start new.
			try {
				lastDate = minuteDateFormatter.parse(lastUpadatedTime);
				currDate = minuteDateFormatter.parse(currentTime);

				if (lastDate.getTime() < currDate.getTime()) {
					dataLoaderService.updateStagingES(lastUpadatedTime, currentTime, null, true);
				} else {
					logger.debug("Waiting to time complete...");
				}
			} catch (Exception e) {
				logger.error("Exception:" + e);
			}
		} else if (status.equalsIgnoreCase("stop")) {
			logger.debug("Event indexing stopped...");
		} else {
			String lastCheckedCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN, 0).getStringValue();
			String lastMaxCount = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_MAX_COUNT, DEFAULT_COLUMN, 0).getStringValue();

			if (Integer.parseInt(lastCheckedCount) < Integer.parseInt(lastMaxCount)) {
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN, EMPTY_STRING + (Integer.parseInt(lastCheckedCount) + 1));
			} else {
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_STATUS, DEFAULT_COLUMN, COMPLETED);
				baseDao.saveStringValue(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN, "" + 0);
			}
		}
	}
	
	@Override
	public void index(String ids, String indexType) throws Exception {
		if (indexType.equalsIgnoreCase("resource")) {
			dataLoaderService.indexResource(ids);
		}
		if (indexType.equalsIgnoreCase("user")) {
			dataLoaderService.indexUser(ids);
		}
		if (indexType.equalsIgnoreCase("event")) {
			dataLoaderService.indexEvent(ids);
		}
		if (indexType.equalsIgnoreCase("code")) {
			dataLoaderService.indexTaxonomy(ids);
		}
	}

	public DataLoggerCaches getLoggerCache() {
		return loggerCache;
	}

	public void setLoggerCache(DataLoggerCaches loggerCache) {
		this.loggerCache = loggerCache;
	}

}
