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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.ColumnFamily;
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
import com.netflix.astyanax.model.Rows;

@Service
public class EventServiceImpl implements EventService {

	protected final Logger logger = LoggerFactory.getLogger(EventServiceImpl.class);

	protected CassandraDataLoader dataLoaderService;
	private final CassandraConnectionProvider connectionProvider;
	private BaseCassandraRepoImpl baseDao;

	public EventServiceImpl() {
		dataLoaderService = new CassandraDataLoader();
		this.connectionProvider = dataLoaderService.getConnectionProvider();
		baseDao = new BaseCassandraRepoImpl(connectionProvider);
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
		ColumnList<String> apiKeyValues = baseDao.readWithKey(ColumnFamily.APIKEY.getColumnFamily(), apiKey, 0);
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
		ColumnList<String> eventColumnList = baseDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventKey, 0);
		return eventColumnList;
	}

	@Override
	public Rows<String, String> readLastNevents(String apiKey, Integer rowsToRead) {
		Rows<String, String> eventRowList = baseDao.readIndexedColumnLastNrows(ColumnFamily.EVENTDETAIL.getColumnFamily(), "api_key", apiKey, rowsToRead, 0);
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
	public List<Map<String, Object>> readUserLastNEventsResourceIds(String userUid, String startTime, String endTime, String eventName, Integer eventsToRead) {
		String activity = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> valueList = new ArrayList<Map<String, Object>>();
		JsonElement jsonElement = null;
		ColumnList<String> activityJsons;

		if (eventName != null) {
			startColumnPrefix = startTime + "~" + eventName;
			endColumnPrefix = endTime + "~" + eventName;
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
					jsonElement = new JsonParser().parse(activity.toString());
					JsonObject eventObj = jsonElement.getAsJsonObject();

					if (eventObj.get("content_gooru_oid") != null) {
						valueMap.put("resourceId", eventObj.get("content_gooru_oid").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("parent_gooru_oid") != null) {
						valueMap.put("parentId", eventObj.get("parent_gooru_oid").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("event_name") != null) {
						valueMap.put("eventName", eventObj.get("event_name").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("user_uid") != null) {
						valueMap.put("userUid", eventObj.get("user_uid").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("username") != null) {
						valueMap.put("username", eventObj.get("username").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("score") != null) {
						valueMap.put("score", eventObj.get("score").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("session_id") != null) {
						valueMap.put("sessionId", eventObj.get("session_id").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("first_attempt_status") != null) {
						valueMap.put("firstAttemptStatus", eventObj.get("first_attempt_status").toString().replaceAll("\"", ""));
					}
					if (eventObj.get("answer_status") != null) {
						valueMap.put("answerStatus", eventObj.get("answer_status").toString().replaceAll("\"", ""));
					}
					/*
					 * if(eventObj.get("date_time") != null){
					 * valueMap.put("dateTime",
					 * eventObj.get("date_time").toString().replaceAll("\"",
					 * "")); }
					 */
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
	public ActionResponseDTO<EventObject> handleEventObjectMessage(EventObject eventObject) throws JSONException, ConnectionException, IOException, GeoIp2Exception {

		Errors errors = validateInsertEventObject(eventObject);
		if (!errors.hasErrors()) {
			// eventObjectValidator.validateEventObject(eventObject);
			dataLoaderService.handleEventObjectMessage(eventObject);
		}
		return new ActionResponseDTO<EventObject>(eventObject, errors);
	}

	@Async
	public void watchSession() {
		dataLoaderService.watchSession();
	}

	@Async
	public void executeForEveryMinute(String startTime, String endTime) {
		dataLoaderService.executeForEveryMinute(startTime, endTime);
	}

	public boolean createEvent(String eventName, String apiKey) {
		return dataLoaderService.createEvent(eventName, apiKey);
	}

	public boolean validateSchedular() {

		return dataLoaderService.validateSchedular();
	}

	public void clearCache() {
		dataLoaderService.clearCache();
	}

	public void migrateCF(String cfName) {
		dataLoaderService.migrateCF(cfName);
	}
}
