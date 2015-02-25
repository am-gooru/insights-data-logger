/*******************************************************************************
 * EventController.java
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
package org.logger.event.web.controller;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.EventData;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.web.controller.dto.ActionResponseDTO;
import org.logger.event.web.service.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Controller
@RequestMapping(value = "/event")
@EnableAsync
public class EventController implements Constants {

	protected final Logger logger = LoggerFactory.getLogger(EventController.class);

	@Autowired
	protected EventService eventService;

	private Gson gson = new Gson();

	/**
	 * Tracks events.
	 * 
	 * @param fields
	 *            Request Body with a JSON string that will have the fields to update
	 * @param eventName
	 *            Name of the event
	 * @param apiKey
	 *            API Key for the logging service
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	@RequestMapping(method = RequestMethod.POST, headers = "Content-Type=application/json")
	public void trackEvent(@RequestBody String fields, @RequestParam(value = API_KEY, required = true) String apiKey, HttpServletRequest request, HttpServletResponse response) throws Exception {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		boolean isValid = ensureValidRequest(request, response);
		if (!isValid) {
			sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, INVALID_API_KEY);
			return;
		}

		EventData eventData = null;
		JsonElement jsonElement = null;
		JsonArray eventJsonArr = null;
		ActionResponseDTO<EventData> responseDTO = null;
		ActionResponseDTO<Event> eventResultDTO = null;

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

		request.setCharacterEncoding("UTF-8");
		Long timeStamp = System.currentTimeMillis();
		String userAgent = request.getHeader("User-Agent");

		String userIp = request.getHeader("X-FORWARDED-FOR");
		if (userIp == null) {
			userIp = request.getRemoteAddr();
		}

		for (JsonElement eventJson : eventJsonArr) {
			JsonObject eventObj = eventJson.getAsJsonObject();
			String eventString = eventObj.toString();
			if (eventObj.get(VERSION) == null) {
				eventData = new EventData();
				eventData.setStartTime(timeStamp);
				eventData.setEndTime(timeStamp);
				eventData.setApiKey(apiKey);
				eventData.setUserAgent(userAgent);
				eventData.setUserIp(userIp);
				eventData.setEventSource(EVENT_SOURCE);
				eventData.setFields(eventString);
				responseDTO = this.createEventData(responseDTO, eventData, eventObj);

				if (responseDTO.getErrors().getErrorCount() > 0) {
					response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
					throw new IllegalArgumentException(responseDTO.getErrors().getFieldError().getDefaultMessage());
				}
			} else {
				Event event = gson.fromJson(eventObj, Event.class);
				JSONObject field = new JSONObject(eventString);
				if(StringUtils.isNotBlank(event.getUser())){
					JSONObject user = new JSONObject(event.getUser());
					user.put(USER_IP, userIp);
					user.put(USER_AGENT, userAgent);
					field.put(USER, user.toString());
				}
				event.setFields(field.toString());
				event.setApiKey(apiKey);
				eventResultDTO = eventService.processMessage(event);
				if (eventResultDTO != null && eventResultDTO.getErrors().getErrorCount() > 0) {
					response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
					throw new IllegalArgumentException(eventResultDTO.getErrors().getFieldError().getDefaultMessage());
				}
			}
		}

		return;
	}

	/**
	 * Create eventData object by iterating json
	 * 
	 * @param responseDTO
	 * @param eventData
	 * @param eventObj
	 * @return
	 */
	private ActionResponseDTO<EventData> createEventData(ActionResponseDTO<EventData> responseDTO, EventData eventData, JsonObject eventObj) {

		if (eventObj.get(EVENT_NAME) != null) {
			eventData.setEventName(eventObj.get(EVENT_NAME).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(EVENT_ID) != null) {
			eventData.setEventId(eventObj.get(EVENT_ID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(EVENT_ID) != null) {
			eventData.setEventId(eventObj.get(EVENT_ID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(CONTENT_GOORU_OID) != null) {
			eventData.setContentGooruId(eventObj.get(CONTENT_GOORU_OID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(PARENT_GOORU_OID) != null) {
			eventData.setParentGooruId(eventObj.get(PARENT_GOORU_OID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(PARENT_EVENT_ID) != null) {
			eventData.setParentEventId(eventObj.get(PARENT_EVENT_ID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(ORGANIZATION_UID) != null) {
			eventData.setOrganizationUid(eventObj.get(ORGANIZATION_UID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(EVENT_TYPE) != null) {
			eventData.setEventType(eventObj.get(EVENT_TYPE).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(TYPE) != null) {
			eventData.setType(eventObj.get(TYPE).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(GOORUID) != null) {
			eventData.setType(eventObj.get(GOORUID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING));
		}
		if (eventObj.get(TIMESPENTINMS) != null) {
			eventData.setTimeSpentInMs(Long.parseLong(eventObj.get(TIMESPENTINMS).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING)));
		}
		if (eventObj.get(ATTMPT_TRY_SEQ) != null) {
			JsonArray jsonArray = eventObj.get(ATTMPT_TRY_SEQ).getAsJsonArray();
			int[] attempTrySequence = new int[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				attempTrySequence[i] = jsonArray.get(i).getAsInt();
			}

			eventData.setAttemptTrySequence(attempTrySequence);
		}

		if (eventObj.get(ATTMPT_STATUS) != null) {
			JsonArray jsonArray = eventObj.get(ATTMPT_STATUS).getAsJsonArray();
			int[] attemptStatus = new int[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				attemptStatus[i] = jsonArray.get(i).getAsInt();
			}
			eventData.setAttemptStatus(attemptStatus);
		}
		if (eventObj.get(ANSWER_ID) != null) {
			JsonArray jsonArray = eventObj.get(ANSWER_ID).getAsJsonArray();
			int[] answerId = new int[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				answerId[i] = jsonArray.get(i).getAsInt();
			}
			eventData.setAnswerId(answerId);
		}
		if (eventObj.get(OPEN_ENDED_TEXT) != null) {
			eventData.setOpenEndedText((eventObj.get(OPEN_ENDED_TEXT).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING)));
		}
		if (eventObj.get(CONTEXT_INFO) != null) {
			eventData.setContextInfo((eventObj.get(CONTEXT_INFO).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING)));
		}
		if (eventObj.get(COLLABORATOR_IDS) != null) {
			eventData.setCollaboratorIds((eventObj.get(COLLABORATOR_IDS).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING)));
		}
		if (eventObj.get(MOBILE_DATA) != null) {
			eventData.setMobileData(Boolean.parseBoolean((eventObj.get(MOBILE_DATA).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING))));
		}
		if (eventObj.get(HINT_ID) != null) {
			eventData.setHintId(Integer.parseInt((eventObj.get(HINT_ID).toString().replaceAll(FORWARD_SLASH, EMPTY_STRING))));
		}
		// push it to cassandra
		responseDTO = eventService.handleLogMessage(eventData);

		return responseDTO;
	}

	/**
	 * Validating apiKey
	 * 
	 * @param request
	 * @param response
	 * @return
	 */
	private boolean ensureValidRequest(HttpServletRequest request, HttpServletResponse response) {

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO validKey = eventService.verifyApiKey(apiKeyToken);
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
	private void sendErrorResponse(HttpServletRequest request, HttpServletResponse response, int responseStatus, String message) {
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

	/**
	 * 
	 * @param request
	 * @param response
	 */
	@RequestMapping(value = "/authenticate", method = RequestMethod.POST)
	public void authenticateRequest(HttpServletRequest request, HttpServletResponse response) {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO appDO = eventService.verifyApiKey(apiKeyToken);
			if (appDO != null) {
				response.setContentType("application/json");
				Map<String, Object> resultMap = new HashMap<String, Object>();

				resultMap.put("endPoint", appDO.getEndPoint());
				resultMap.put("pushIntervalMs", appDO.getDataPushingIntervalInMillsecs());
				JSONObject resultJson = new JSONObject(resultMap);

				try {
					response.getWriter().write(resultJson.toString());
				} catch (IOException e) {
					logger.error("OOPS! Something went wrong", e);
				}

				return;
			}
		}
		sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
		return;
	}

	/**
	 * Read events from event detail
	 * 
	 * @param request
	 * @param apiKey
	 * @param eventId
	 * @param response
	 */
	@RequestMapping(value = "/tail", method = RequestMethod.GET)
	public void readEventDetails(HttpServletRequest request, @RequestParam(value = "apiKey", required = true) String apiKey, @RequestParam(value = EVENT_ID, required = true) String eventId,
			HttpServletResponse response) {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO appDO = eventService.verifyApiKey(apiKeyToken);
			if (appDO != null) {
				ColumnList<String> eventDetail = eventService.readEventDetail(eventId);
				if (eventDetail != null && !eventDetail.isEmpty()) {

					response.setContentType("application/json");
					Map<String, Object> resultMap = new HashMap<String, Object>();

					resultMap.put("eventJSON", eventDetail.getStringValue("fields", null));
					resultMap.put("startTime", eventDetail.getLongValue("start_time", null));
					resultMap.put("endTime", eventDetail.getLongValue("end_time", null));
					resultMap.put(EVENT_NAME, eventDetail.getStringValue("event_name", null));
					resultMap.put("apiKey", eventDetail.getStringValue("api_key", null));
					JSONObject resultJson = new JSONObject(resultMap);

					try {
						response.getWriter().write(resultJson.toString());
					} catch (IOException e) {
						logger.error("OOPS! Something went wrong", e);
					}

				}
				return;
			}
		}
		sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
		return;

	}

	/**
	 * Clearing cached data
	 * 
	 * @param request
	 * @param response
	 */
	@RequestMapping(value = "/clear/cache", method = RequestMethod.GET)
	public void clearCache(HttpServletRequest request, HttpServletResponse response) {
		try {
			eventService.clearCache();
			sendErrorResponse(request, response, HttpServletResponse.SC_OK, "Cleared Cache");
		} catch (Exception e) {
			sendErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something wrong");
		}
	}
	
	/**
	 * DI Indexing api for all type.
	 * @param request
	 * @param indexType
	 * @param ids
	 * @param resourceType
	 * @param response
	 */
	@RequestMapping(value = "/index/{type}", method = RequestMethod.GET)
	public void indexResource(HttpServletRequest request,@PathVariable(value="type") String indexType, @RequestParam(value = "ids", required = true) String ids,@RequestParam(value = "resourceType", required = false) String resourceType ,HttpServletResponse response) {
			try {
				eventService.index(ids, indexType);
			} catch (Exception e) {
				sendErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Oops!!Indexing failed!!"+e);
			}
			sendErrorResponse(request, response, HttpServletResponse.SC_OK, "Indexed successfully!!");
	}

	/**
	 * Get last few events
	 * 
	 * @param request
	 * @param apiKey
	 * @param totalRows
	 * @param response
	 */
	@RequestMapping(value = "/latest/tail", method = RequestMethod.GET)
	public void readLastNevents(HttpServletRequest request, @RequestParam(value = "apiKey", required = true) String apiKey,
			@RequestParam(value = "totalRows", defaultValue = "20", required = false) Integer totalRows, HttpServletResponse response) {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO appDO = eventService.verifyApiKey(apiKeyToken);
			if (appDO != null) {
				Rows<String, String> eventDetailRows = eventService.readLastNevents(apiKey, totalRows);
				if (eventDetailRows != null && !eventDetailRows.isEmpty()) {

					List<Map<String, Object>> eventJSONList = new ArrayList<Map<String, Object>>();

					response.setContentType("application/json");

					// Iterate through cassandra rows and get the event JSONS
					for (Row<String, String> row : eventDetailRows) {
						Map<String, Object> resultMap = new HashMap<String, Object>();
						resultMap.put("eventJSON", row.getColumns().getStringValue("fields", null));
						resultMap.put("startTime", row.getColumns().getLongValue("start_time", null));
						resultMap.put("endTime", row.getColumns().getLongValue("end_time", null));
						resultMap.put(EVENT_NAME, row.getColumns().getStringValue("event_name", null));
						resultMap.put("apiKey", row.getColumns().getStringValue("api_key", null));
						eventJSONList.add(resultMap);
					}
					JSONObject resultJson = new JSONObject(eventJSONList);

					try {
						response.getWriter().write(resultJson.toString());
					} catch (IOException e) {
						logger.error("OOPS! Something went wrong", e);
					}

				}
				return;
			}
		}
		sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
		return;

	}

	/**
	 * run micro aggregation for the given time range
	 */
	public void runMicroAggregation() {
		if (!validateSchedular()) {
			return;
		}
		eventService.runMicroAggregation(null, null);
	}

	/**
	 * run micro aggregation for the given time range
	 */
	public void runMicroAggregation(String startTime, String endTime) {
		if (!validateSchedular()) {
			return;
		}
		eventService.runMicroAggregation(startTime, endTime);
	}

	/**
	 * Indexing events in ES
	 */
	public void indexActivity() {
		if (!validateSchedular()) {
			return;
		}
		eventService.indexActivity();
	}

	/**
	 * 
	 * @param userUid
	 * @param apiKey
	 * @param eventName
	 * @param minutesToRead
	 * @param eventsToRead
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	@RequestMapping(value = "/latest/activity", method = RequestMethod.GET)
	public void getUserActivity(@RequestParam String userUid, @RequestParam(value = "apiKey", required = false) String apiKey, @RequestParam(value = EVENT_NAME, required = false) String eventName,
			@RequestParam(value = "minutesToRead", required = false, defaultValue = "30") Integer minutesToRead,
			@RequestParam(value = "eventsToRead", required = false, defaultValue = "30") Integer eventsToRead, HttpServletRequest request, HttpServletResponse response) throws Exception {

		SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		Calendar cal = Calendar.getInstance();
		String currentMinute = minuteDateFormatter.format(cal.getTime());
		cal.setTime(minuteDateFormatter.parse(currentMinute));
		cal.add(Calendar.MINUTE, -minutesToRead);
		Date decrementedTime = cal.getTime();
		String decremenedMinute = minuteDateFormatter.format(decrementedTime);

		List<Map<String, Object>> resultMap = eventService.readUserLastNEventsResourceIds(userUid, decremenedMinute, currentMinute, eventName, eventsToRead);
		JSONObject resultJson = new JSONObject();
		resultJson.put("activity", resultMap);
		try {
			response.getWriter().write(resultJson.toString());
		} catch (IOException e) {
			logger.error("OOPS! Something went wrong", e);
		}
		return;
	}

	/**
	 * 
	 * @param request
	 * @param apiKey
	 * @param eventName
	 * @param response
	 * @throws IOException
	 */
	@RequestMapping(method = RequestMethod.PUT)
	public void createEvent(HttpServletRequest request, @RequestParam(value = "apiKey", required = true) String apiKey, @RequestParam(value = EVENT_NAME, required = true) String eventName,
			HttpServletResponse response) throws IOException {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "PUT");

		boolean isValid = ensureValidRequest(request, response);
		if (!isValid) {
			sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
			return;
		}

		response.setContentType("application/json");
		if (!eventName.contains(".") || eventName.startsWith(".")) {
			sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid Event Name it should be noun.verb ");
			return;
		}
		Map<String, String> status = new HashMap<String, String>();
		if (eventService.createEvent(eventName, apiKey)) {
			status.put(EVENT_NAME, eventName);
			status.put("status", "Created");
			response.getWriter().write(new JSONObject(status).toString());
		} else {
			sendErrorResponse(request, response, HttpServletResponse.SC_CONFLICT, " Event Already Exists : " + eventName);
			return;
		}

	}

	/**
	 * Validating for scheduler can run in this server or not
	 * @return
	 */
	public boolean validateSchedular() {
		boolean value = eventService.validateSchedular();
		logger.debug("Can run scheduler? : "+value);
		return value;
	}
}
