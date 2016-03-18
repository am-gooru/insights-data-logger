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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ednovo.data.model.AppDO;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.web.service.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping(value = "/event")
@EnableAsync
public class EventController implements AsyncConfigurer {

	protected final Logger logger = LoggerFactory.getLogger(EventController.class);

	@Autowired
	protected EventService eventService;

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
	public void trackEvent(@RequestBody String fields, @RequestParam(value = Constants.API_KEY, required = true) String apiKey, HttpServletRequest request, HttpServletResponse response) throws Exception {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");
		eventService.eventLogging(request, response, fields, apiKey);
		return;
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
		eventService.sendErrorResponse(request, response, HttpServletResponse.SC_FORBIDDEN, "Invalid API Key");
		return;
	}

	/**
	 * run micro aggregation for the given time range
	 */
	public void runMicroAggregation() {
		
	}

	/**
	 * run micro aggregation for the given time range
	 */
	public void runMicroAggregation(String startTime, String endTime) {
		
	}

	/**
	 * Indexing events in ES
	 */
	public void indexActivity() {
	
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

     @Override
	public Executor getAsyncExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(15);
		executor.setMaxPoolSize(80);
		executor.setQueueCapacity(200);
		executor.setThreadNamePrefix("eventExecutor");
		executor.initialize();
		return executor;
	}
}
