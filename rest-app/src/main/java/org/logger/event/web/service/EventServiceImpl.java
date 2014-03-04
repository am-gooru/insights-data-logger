/*******************************************************************************
 * EventServiceImpl.java
 * rest-app
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

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.EventData;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.dao.APIDAOCassandraImpl;
import org.logger.event.cassandra.loader.dao.EventDetailDAOCassandraImpl;
import org.logger.event.web.controller.dto.ActionResponseDTO;
import org.logger.event.web.utils.ServerValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.validation.BindException;
import org.springframework.validation.Errors;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class EventServiceImpl implements EventService {

    protected CassandraDataLoader dataLoaderService;
    protected final Logger logger = LoggerFactory.getLogger(EventServiceImpl.class);
    private final CassandraConnectionProvider connectionProvider;
    private APIDAOCassandraImpl apiDao;
    private EventDetailDAOCassandraImpl eventDetailDao;

    public EventServiceImpl() {
        dataLoaderService = new CassandraDataLoader();
        
        this.connectionProvider = dataLoaderService.getConnectionProvider();

        apiDao = new APIDAOCassandraImpl(connectionProvider);
        eventDetailDao = new EventDetailDAOCassandraImpl(connectionProvider);
    }

    @Override
    public ActionResponseDTO<EventData> handleLogMessage(EventData eventData) {

        Errors errors = validateInsertEventData(eventData);

        if (!errors.hasErrors()) {
        	
           // dataLoaderService.handleLogMessage(eventData.getFields(), eventData.getStartTime(), eventData.getUserAgent(), eventData.getUserIp(), eventData.getEndTime(), eventData.getApiKey(), eventData.getEventName(), eventData.getGooruOId(), eventData.getContentId(), eventData.getQuery(), eventData.getGooruUId(), eventData.getUserId(),eventData.getGooruId(),eventData.getType(),eventData.getParentEventId(),eventData.getContext(),eventData.getReactionType(),eventData.getOrganizationUid(),eventData.getTimeSpentInMs(),eventData.getAnswerId(),eventData.getAttemptStatus(),eventData.getAttemptTrySequence(),eventData.getRequestMethod(), eventData.getEventId());
            dataLoaderService.handleLogMessage(eventData);
        }

        return new ActionResponseDTO<EventData>(eventData, errors);
    }
    
    @Override
    public AppDO verifyApiKey(String apiKey) {
        ColumnList<String> apiKeyValues = apiDao.readApiData(apiKey); 
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

	@Override
	public ColumnList<String> readEventDetail(String eventKey) {
		ColumnList<String> eventColumnList = eventDetailDao.readEventDetail(eventKey);
		return eventColumnList;
	}

	@Override
	public Rows<String, String> readLastNevents(String apiKey,
			Integer rowsToRead) {
		Rows<String, String> eventRowList = eventDetailDao.readLastNrows(apiKey, rowsToRead);
		return eventRowList;
	}

	@Override
	public void updateProdViews() {
		dataLoaderService.callAPIViewCount();
	}

}
