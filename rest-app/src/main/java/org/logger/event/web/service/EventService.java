/*******************************************************************************
 * EventService.java
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
import java.util.List;
import java.util.Map;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.json.JSONException;
import org.logger.event.web.controller.dto.ActionResponseDTO;
import org.springframework.stereotype.Service;

import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public interface EventService {

	public ActionResponseDTO<EventData> handleLogMessage(EventData eventData);

	public ActionResponseDTO<EventObject> handleEventObjectMessage(EventObject eventObject) throws JSONException, ConnectionException, IOException, GeoIp2Exception;

	public AppDO verifyApiKey(String apiKeyToken);

	public ColumnList<String> readEventDetail(String eventKey);

	public Rows<String, String> readLastNevents(String apiKey, Integer rowsToRead);

	public void updateProdViews();
	
	List<Map<String, Object>> readUserLastNEventsResourceIds(String apiKey, String userUid, String rowsToRead, String eventName, Integer eventsToRead);

	public void watchSession();
	
	public void indexActivity();

	void executeForEveryMinute(String startTime, String endTime);
	
	boolean createEvent(String eventName,String apiKey);
	
	boolean validateSchedular(String ipAddress);

	void postMigration(String start,String Stop,String param);
	
	void postStatMigration(String start,String Stop,String param);
	
	void catalogMigration(String start,String Stop,String param);
	void balanceStatDataUpdate();
	
	public void clearCacher();
	
	public void indexResource(String ids);

	public void indexTaxonomy(String ids) throws Exception;
	
	public void indexUser(String ids) throws Exception;
	
	public void indexEvents(String ids) throws Exception;
	
	public void indexResourceViews(String ids,String type) throws Exception;
}
