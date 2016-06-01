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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.EventBuilder;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.logger.event.web.utils.ServerValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

@Service
public class EventServiceImpl implements EventService {

	protected final Logger logger = LoggerFactory.getLogger(EventServiceImpl.class);

	protected CassandraDataLoader dataLoaderService;
	private BaseCassandraRepo baseDao;
	private SimpleDateFormat minuteDateFormatter;
	
	public EventServiceImpl() {
		baseDao = BaseCassandraRepo.instance();
		dataLoaderService = new CassandraDataLoader();
		this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		minuteDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	/**
	 * 
	 * @param event
	 * @return
	 */
	@Async
	private Boolean validateInsertEvent(EventBuilder event) {
		Boolean isValidEvent = true;
		if (event == null) {
			ServerValidationUtils.logErrorIfNull(isValidEvent, event, "event.all", Constants.RAW_EVENT_NULL_EXCEPTION);
		}
		String eventJson = event.getFields();
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getEventName(), Constants.EVENT_NAME, "LA001", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getEventId(), Constants.EVENT_ID, "LA002", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getVersion(), Constants.VERSION, "LA003", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getUser(), Constants.USER, "LA004", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getSession(), Constants.SESSION, "LA005", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getMetrics(), Constants.METRICS, "LA006", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getContext(), Constants.CONTEXT, "LA007", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfNullOrEmpty(isValidEvent, event.getPayLoadObject().toString(), Constants.PAY_LOAD, "LA008", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfZeroLongValue(isValidEvent, event.getStartTime(), Constants.START_TIME, "LA009", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.logErrorIfZeroLongValue(isValidEvent, event.getEndTime(), Constants.END_TIME, "LA010", eventJson, Constants.RAW_EVENT_NULL_EXCEPTION);
		ServerValidationUtils.deepEventCheck(isValidEvent, event, eventJson);
		return isValidEvent;
	}

	/**
	 * Validating apiKey
	 * 
	 * @param request
	 * @param response
	 * @return
	 */
	public boolean ensureValidRequest(String apiKeyToken) {
		logger.info("apiKeyToken" + apiKeyToken);
		logger.info("apiKeyToken length" + apiKeyToken.length());
		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			logger.info("apiKeyToken is not null....");
			AppDO validKey = verifyApiKey(apiKeyToken);
			logger.info("validKey is not null...." + validKey);
			if (validKey != null) {
				return true;
			}
		}
		return false;
	}

	@Override
	@Async
	public void eventLogging(HttpServletRequest request,  String fields, String apiKey) {
		boolean isValid = ensureValidRequest(apiKey);
		if (!isValid) {
			logger.error("inValid request..");
			return;
		}
		JsonElement jsonElement = null;
		JsonArray eventJsonArr = null;
		if (!fields.isEmpty()) {

			try {
				// validate JSON
				logger.info("validate JSON");
				jsonElement = new JsonParser().parse(fields);
				eventJsonArr = jsonElement.getAsJsonArray();
			} catch (JsonParseException e) {
				// Invalid.
				logger.error(Constants.INVALID_JSON, e);
				return;
			}

		} else {
			logger.error("Field should not be empty");
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
				EventBuilder event = new EventBuilder(eventString);
					if (event.getUser() != null && event.getUser().length() > 0) {
						JSONObject user = event.getUser();
						user.put(Constants.USER_IP, userIp);
						user.put(Constants.USER_AGENT, userAgent);
						event.setUser(user);
						
					}
					event.setFields((new JSONObject(eventString).put(Constants.USER, event.getUser())).toString());
					event.setApiKey(apiKey);
					processMessage(event);
			}
			Thread.sleep(2000);
		} catch (Exception e) {
			logger.error("Exception : ", e);
		}
	}
	@Async
	private void processMessage(EventBuilder event){
		Boolean isValidEvent = validateInsertEvent(event);
		if (isValidEvent) {
			dataLoaderService.processMessage(event);
		}
	}
	private AppDO verifyApiKey(String apiKey) {		
		return baseDao.getApiKeyDetails(apiKey);
	}

}
