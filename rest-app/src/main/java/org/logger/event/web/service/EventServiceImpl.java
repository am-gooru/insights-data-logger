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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.EventData;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.logger.event.web.controller.dto.ActionResponseDTO;
import org.logger.event.web.utils.ServerValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.validation.BindException;
import org.springframework.validation.Errors;

import com.google.gson.JsonArray;
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

	@Autowired
	private CassandraDataLoader dataLoaderService;
	
	@Autowired	
	private BaseCassandraRepoImpl baseDao;
	
	private SimpleDateFormat minuteDateFormatter;
	
	@Autowired
	private DataLoggerCaches loggerCache;
	
	public EventServiceImpl() {
		this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		this.minuteDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
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
		ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamilySet.APIKEY.getColumnFamily(), apiKey);
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
		String eventJson = event.getFields();
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getEventName(), EVENT_NAME, "LA001", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getEventId(), EVENT_ID, "LA002", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getVersion(), VERSION, "LA003", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getUser(), USER, "LA004", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getSession(), SESSION, "LA005", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getMetrics(), METRICS, "LA006", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getContext(), CONTEXT, "LA007", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getPayLoadObject().toString(), PAY_LOAD, "LA008", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfZeroLongValue(isValidEvent, event.getStartTime(), START_TIME, "LA009", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfZeroLongValue(isValidEvent, event.getEndTime(), END_TIME, "LA010", eventJson, RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.deepEventCheck(isValidEvent, event, eventJson);
		return isValidEvent;
	}

	@Override
	public ColumnList<String> readEventDetail(String eventKey) {
		ColumnList<String> eventColumnList = baseDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), eventKey);
		return eventColumnList;
	}

	@Override
	public Rows<String, String> readLastNevents(String apiKey, Integer rowsToRead) {
		Rows<String, String> eventRowList = baseDao.readIndexedColumnLastNrows(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), "api_key", apiKey, rowsToRead);
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

		activityJsons = baseDao.readColumnsWithPrefix(ColumnFamilySet.ACTIVITYSTREAM.getColumnFamily(), userUid, startColumnPrefix, endColumnPrefix, eventsToRead);
		if ((activityJsons == null || activityJsons.isEmpty() || activityJsons.size() == 0 || activityJsons.size() < 30) && eventName == null) {
			activityJsons = baseDao.readKeyLastNColumns(ColumnFamilySet.ACTIVITYSTREAM.getColumnFamily(), userUid, eventsToRead);
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

	/**
	 * Validating apiKey
	 * 
	 * @param request
	 * @param response
	 * @return
	 */
	public boolean ensureValidRequest(HttpServletRequest request, HttpServletResponse response) {

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO validKey = verifyApiKey(apiKeyToken);
			if (validKey != null) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * @param request
	 * @param response
	 * @param responseStatus
	 * @param message
	 */
	public void sendErrorResponse(HttpServletRequest request, HttpServletResponse response, int responseStatus, String message) {
		response.setStatus(responseStatus);
		response.setContentType("application/json");
		Map<String, Object> resultMap = new HashMap<String, Object>();

		resultMap.put("statusCode", responseStatus);
		resultMap.put("message", message);
		JSONObject resultJson = new JSONObject(resultMap);

		try {
			response.getWriter().write(resultJson.toString());
		} catch (IOException e) {
			logger.error("OOPS! Something went wrong", e);
		}
	}

	@Override
	@Async
	public void eventLogging(HttpServletRequest request, HttpServletResponse response, String fields, String apiKey) {
		boolean isValid = ensureValidRequest(request, response);
		if (!isValid) {
			sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, INVALID_API_KEY);
			return;
		}
		JsonElement jsonElement = null;
		JsonArray eventJsonArr = null;
		if (!fields.isEmpty()) {

			try {
				// validate JSON
				jsonElement = new JsonParser().parse(fields);
				eventJsonArr = jsonElement.getAsJsonArray();
			} catch (JsonParseException e) {
				// Invalid.
				sendErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST, INVALID_JSON);
				logger.error(INVALID_JSON, e);
				return;
			}

		} else {
			sendErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST, BAD_REQUEST);
			return;
		}

		try {
			request.setCharacterEncoding("UTF-8");
			String userAgent = request.getHeader("User-Agent");

			String userIp = request.getHeader("X-FORWARDED-FOR");
			if (userIp == null) {
				userIp = request.getRemoteAddr();
			}
			for (JsonElement eventJson : eventJsonArr) {
				JsonObject eventObj = eventJson.getAsJsonObject();
				String eventString = eventObj.toString();				
				Event event = new Event(eventString);
					if (event.getUser() != null && event.getUser().length() > 0) {
						JSONObject user = event.getUser();
						user.put(USER_IP, userIp);
						user.put(USER_AGENT, userAgent);
						event.put(USER, user);
						
					}
					event.setFields((new JSONObject(eventString).put(USER, event.getUser())).toString());
					event.setApiKey(apiKey);
					processMessage(event);
			}
		} catch (Exception e) {
			logger.error("Exception : ", e);
		}

	}
	@Async
	private void processMessage(Event event){
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
		String lastUpadatedTime = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_LAST_UPDATED, DEFAULT_COLUMN).getStringValue();
		String currentTime = minuteDateFormatter.format(new Date());
		logger.info("lastUpadatedTime: " + lastUpadatedTime + " - currentTime: " + currentTime);
		Date lastDate = null;
		Date currDate = null;
		String status = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_STATUS, DEFAULT_COLUMN).getStringValue();
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
				logger.error("Exception:" , e);
			}
		} else if (status.equalsIgnoreCase("stop")) {
			logger.debug("Event indexing stopped...");
		} else {
			String lastCheckedCount = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN).getStringValue();
			String lastMaxCount = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_MAX_COUNT, DEFAULT_COLUMN).getStringValue();

			if (Integer.parseInt(lastCheckedCount) < Integer.parseInt(lastMaxCount)) {
				baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN, EMPTY_STRING + (Integer.parseInt(lastCheckedCount) + 1));
			} else {
				baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_STATUS, DEFAULT_COLUMN, COMPLETED);
				baseDao.saveStringValue(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), ACTIVITY_INDEX_CHECKED_COUNT, DEFAULT_COLUMN, "" + 0);
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
