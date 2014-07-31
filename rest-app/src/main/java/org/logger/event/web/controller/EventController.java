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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.web.controller.dto.ActionResponseDTO;
import org.logger.event.web.service.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Controller;
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
@RequestMapping(value="/event")
@EnableAsync
public class EventController {

	protected final Logger logger = LoggerFactory 
			.getLogger(EventController.class);
	private final String EVENT_SOURCE = "api-logged";

	@Autowired
	protected EventService eventService;
	
	private Gson gson = new Gson();
	
	/**
	 * Tracks events. 
	 * @param fields Request Body with a JSON string that will have the fields to update
	 * @param eventName Name of the event
	 * @param apiKey API Key for the logging service
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	@RequestMapping(method = RequestMethod.POST, headers="Content-Type=application/json")
	public void trackEvent(
			@RequestBody String fields,
			@RequestParam(value = "apiKey", required = true) String apiKey,
			HttpServletRequest request, HttpServletResponse response)
			throws Exception {
		
		//add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");
		
		boolean isValid = ensureValidRequest(request, response);
		if(!isValid) {
			sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
			return;
		}
		
		EventData eventData = null;
		EventObject eventObject = null;
		JsonElement jsonElement = null;
		JsonArray eventJsonArr = null;
		ActionResponseDTO<EventData>  responseDTO= null;
		ActionResponseDTO<EventObject>  eventObjDTO= null;
		
		if(!fields.isEmpty()){
			
			try {
				//validate JSON
				jsonElement = new JsonParser().parse(fields);
			    eventJsonArr = jsonElement.getAsJsonArray();
			} catch (JsonParseException e) {
			    // Invalid.
				sendErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST, "Invalid JSON");
				logger.error("OOPS! Invalid JSON", e);
				return;
			}
			
		}
		else {
			sendErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST, "Bad Request");
			return;
		}
		
		request.setCharacterEncoding("UTF-8");
		Long timeStamp = System.currentTimeMillis();
		String userAgent = request.getHeader("User-Agent");
		
		String userIp = request.getHeader("X-FORWARDED-FOR");
		if (userIp == null) 
		{
			userIp = request.getRemoteAddr();
		}

		for (JsonElement eventJson : eventJsonArr) {
			eventData = new EventData();
			eventObject = new EventObject();
			eventData.setStartTime(timeStamp);
			eventData.setEndTime(timeStamp);
			eventData.setApiKey(apiKey);
			eventData.setUserAgent(userAgent);
			eventData.setUserIp(userIp);
			eventData.setEventSource(EVENT_SOURCE);
			eventData.setFields(eventJson.getAsJsonObject().toString());
			eventObject.setStartTime(timeStamp);
			eventObject.setEndTime(timeStamp);
			JsonObject eventObj = eventJson.getAsJsonObject();
			if(eventObj.get("version") == null){
				logger.info("Version :{}",eventObj.get("version"));
				responseDTO = this.createEventData(responseDTO,eventData,eventObj);
	
				if (responseDTO.getErrors().getErrorCount() > 0) {
		            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		            throw new IllegalArgumentException(responseDTO.getErrors().getFieldError().getDefaultMessage());
				}
			}else{
				EventObject eventObjects = gson.fromJson(eventObj, EventObject.class);
				JsonObject jsonObj = eventJson.getAsJsonObject();
				eventObjects.setFields(jsonObj.toString());
				JSONObject useObj = new JSONObject(eventObjects.getUser());
				useObj.put("userIp", userIp);
				useObj.put("userAgent", userAgent);
				JSONObject fieldsObj = new JSONObject(eventObjects.getFields());
				fieldsObj.put("user", useObj.toString());
				eventObjects.setFields(fieldsObj.toString());
				eventObjects.setApiKey(apiKey);
	        	 eventObjDTO = eventService.handleEventObjectMessage(eventObjects);
					if (eventObjDTO != null && eventObjDTO.getErrors().getErrorCount() > 0) {
			            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			           throw new IllegalArgumentException(eventObjDTO.getErrors().getFieldError().getDefaultMessage());
					}
				}		
		}
		
		return;
}
	
 private ActionResponseDTO<EventData> createEventData(ActionResponseDTO<EventData> responseDTO,EventData eventData ,JsonObject eventObj) {
		
		if(eventObj.get("eventName") != null){
			eventData.setEventName(eventObj.get("eventName").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("eventId") != null){
			eventData.setEventId(eventObj.get("eventId").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("eventId") != null){
			eventData.setEventId(eventObj.get("eventId").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("contentGooruId") != null){
			eventData.setContentGooruId(eventObj.get("contentGooruId").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("parentGooruId") != null){
			eventData.setParentGooruId(eventObj.get("parentGooruId").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("parentEventId") != null){
			eventData.setParentEventId(eventObj.get("parentEventId").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("organizationUid") != null){
			eventData.setOrganizationUid(eventObj.get("organizationUid").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("eventType") != null){
			eventData.setEventType(eventObj.get("eventType").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("type") != null){
			eventData.setType(eventObj.get("type").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("gooruUId") != null){
			eventData.setType(eventObj.get("gooruUId").toString().replaceAll("\"", ""));
		}
		if(eventObj.get("timeSpentInMs") != null){
			eventData.setTimeSpentInMs(Long.parseLong(eventObj.get("timeSpentInMs").toString().replaceAll("\"", "")));
		}
		if(eventObj.get("attemptTrySequence") != null){				
			JsonArray jsonArray = eventObj.get("attemptTrySequence").getAsJsonArray();
			int[] attempTrySequence = new int[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				attempTrySequence[i] = jsonArray.get(i).getAsInt();
			}
			
			eventData.setAttemptTrySequence(attempTrySequence);
		}

		if(eventObj.get("attemptStatus") != null){
			JsonArray jsonArray = eventObj.get("attemptStatus").getAsJsonArray();
			int[] attemptStatus = new int[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				attemptStatus[i] = jsonArray.get(i).getAsInt();
			}
			eventData.setAttemptStatus(attemptStatus);
		}
		if(eventObj.get("answerId") != null){
			JsonArray jsonArray = eventObj.get("answerId").getAsJsonArray();
			int[] answerId = new int[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				answerId[i] = jsonArray.get(i).getAsInt();
			}
			eventData.setAnswerId(answerId);
		}
		if(eventObj.get("openEndedText") != null){
			eventData.setOpenEndedText((eventObj.get("openEndedText").toString().replaceAll("\"", "")));
		}
		if(eventObj.get("contextInfo") != null){
			eventData.setContextInfo((eventObj.get("contextInfo").toString().replaceAll("\"", "")));
		}
		if(eventObj.get("collaboratorIds") != null){
			eventData.setCollaboratorIds((eventObj.get("collaboratorIds").toString().replaceAll("\"", "")));
		}
		if(eventObj.get("mobileData") != null){
			eventData.setMobileData(Boolean.parseBoolean((eventObj.get("mobileData").toString().replaceAll("\"", ""))));
		}
		if(eventObj.get("hintId") != null){
			eventData.setHintId(Integer.parseInt((eventObj.get("hintId").toString().replaceAll("\"", ""))));
		}
		//push it to cassandra
		responseDTO = eventService.handleLogMessage(eventData);
		
		return responseDTO;
}
	private boolean ensureValidRequest(HttpServletRequest request, HttpServletResponse response) {

		String apiKeyToken = request.getParameter("apiKey");
		
		if(apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO validKey = eventService.verifyApiKey(apiKeyToken);
			 if(validKey != null)
			 {
				 return true;
			 }
		 }
		 return false;
	}
	
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
		
		//add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		String apiKeyToken = request.getParameter("apiKey");
		
		if(apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO appDO = eventService.verifyApiKey(apiKeyToken);
			 if(appDO != null)
			 {
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
	
	
	@RequestMapping(value = "/tail", method = RequestMethod.GET)
	public void readEventDetails(HttpServletRequest request,
			@RequestParam(value = "apiKey", required = true) String apiKey,
			@RequestParam(value = "eventId", required = true) String eventId,
			HttpServletResponse response) {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers",
				"Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO appDO = eventService.verifyApiKey(apiKeyToken);
			if (appDO != null) {
				ColumnList<String> eventDetail = eventService
						.readEventDetail(eventId);
				if (eventDetail != null && !eventDetail.isEmpty()) {

					response.setContentType("application/json");
					Map<String, Object> resultMap = new HashMap<String, Object>();

					resultMap.put("eventJSON", eventDetail.getStringValue("fields", null));
					resultMap.put("startTime", eventDetail.getLongValue("start_time", null));
					resultMap.put("endTime", eventDetail.getLongValue("end_time", null));
					resultMap.put("eventName", eventDetail.getStringValue("event_name", null));
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
		sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN,
				"Invalid API Key");
		return;

	}
	
	
	@RequestMapping(value = "/latest/tail", method = RequestMethod.GET)
	public void readLastNevents(HttpServletRequest request,
			@RequestParam(value = "apiKey", required = true) String apiKey,
			@RequestParam(value = "totalRows", defaultValue = "20", required = false) Integer totalRows,			
			HttpServletResponse response) {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers",
				"Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");

		String apiKeyToken = request.getParameter("apiKey");

		if (apiKeyToken != null && apiKeyToken.length() == 36) {
			AppDO appDO = eventService.verifyApiKey(apiKeyToken);
			if (appDO != null) {
				 Rows<String, String> eventDetailRows = eventService
						.readLastNevents(apiKey, totalRows);
				if (eventDetailRows != null && !eventDetailRows.isEmpty()) {
					
					List<Map<String, Object>> eventJSONList = new ArrayList<Map<String, Object>>();
					
					response.setContentType("application/json");
					
					//Iterate through cassandra rows and get the event JSONS
					for (Row<String, String> row : eventDetailRows) {
						Map<String, Object> resultMap = new HashMap<String, Object>();
						resultMap.put("eventJSON", row.getColumns().getStringValue("fields", null));
						resultMap.put("startTime", row.getColumns().getLongValue("start_time", null));
						resultMap.put("endTime", row.getColumns().getLongValue("end_time", null));
						resultMap.put("eventName", row.getColumns().getStringValue("event_name", null));
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
		sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN,
				"Invalid API Key");
		return;

	}

	public void updateViews() throws Exception{
		System.out.println("Executing every five mintues");
		
		eventService.updateProdViews();
	}
	
	// scheduled for every 1 minute
	public void executeForEveryMinute() {
		if (!validateSchedular()) {
			return;
		}
		eventService.executeForEveryMinute(null, null);
	}
	
	//run micro aggregation for the given time range
	public void executeForEveryMinute(String startTime,String endTime){
			
		eventService.executeForEveryMinute(startTime, endTime);
	}
		
	public void runAggregation(){
		
	}
	
	public void watchSession() {
		if (!validateSchedular()) {
			return;
		}
		logger.info("watching session");
		eventService.watchSession();
	}

	public void postMigration(){
		if(!validateSchedular()){
			return;
			}
		logger.info("post migration............");
		//eventService.postMigration(null, null, null);
		//eventService.postStatMigration(null, null, null);
		//eventService.balanceStatDataUpdate();
	}
	
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/latest/activity", method = RequestMethod.GET)
	public void getUserActivity(@RequestParam String userUid,
				@RequestParam(value = "apiKey", required = false) String apiKey,
				@RequestParam(value = "eventName", required = false) String eventName,
				@RequestParam(value = "minutesToRead", required = false, defaultValue= "30") Integer minutesToRead,
				@RequestParam(value = "eventsToRead", required = false, defaultValue= "30") Integer eventsToRead,
				HttpServletRequest request, HttpServletResponse response) throws Exception {

		SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
		Calendar cal = Calendar.getInstance();
	 	String currentMinute = minuteDateFormatter.format(cal.getTime());
	 	cal.setTime(minuteDateFormatter.parse(currentMinute));
    	cal.add(Calendar.MINUTE, -minutesToRead);
    	Date decrementedTime =cal.getTime(); 
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
	
	@RequestMapping(method = RequestMethod.PUT)
	public void createEvent(HttpServletRequest request,
			@RequestParam(value="apiKey",required = true) String apiKey,
			@RequestParam(value="eventName",required = true)String eventName,HttpServletResponse response) throws IOException{
	
		//add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "PUT");
				
		boolean isValid = ensureValidRequest(request, response);
		if(!isValid) {
		sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
		return;
		}
		
		response.setContentType("application/json");
		if(!eventName.contains(".") || eventName.startsWith(".")){
			sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid Event Name it should be noun.verb ");
			return;
		}
		
		Map<String,String>	status = eventService.createEvent(eventName,apiKey);
		status.put("eventName", eventName);
		
		response.getWriter().write(new JSONObject(status).toString());
	}
	
	public boolean validateSchedular(){
		
		try {
            InetAddress ip = InetAddress.getByName("DO-LOGAPI");
            String ipAddress = ip.getHostAddress();
            return eventService.validateSchedular(ipAddress);
    } catch (UnknownHostException e) {
            e.printStackTrace();
    }
		return false;
	}
}
