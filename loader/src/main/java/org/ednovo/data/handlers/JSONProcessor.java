/*******************************************************************************
 * JSONProcessor.java
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
package org.ednovo.data.handlers;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;

import com.google.gson.Gson;

public class JSONProcessor extends BaseDataProcessor implements DataProcessor {

    private Gson gson;

    public JSONProcessor() {
        this.gson = new Gson();
    }

    @Override
    public void handleRow(Object row) throws Exception {
        if (row == null) {
            LOG.error("The row was null. This is invalid");
            return;
        }

        String logMessage = (String) row;
        
        // Log lines at times have spaces. Trim them away.
        logMessage = logMessage.trim();
        String jsonRowObject = logMessage;
        if (logMessage.contains("{") && !logMessage.startsWith("{") && !logMessage.startsWith("\"{")) {
            jsonRowObject = StringUtils.substringAfter(logMessage, "activityLog - ");
            if (StringUtils.isEmpty(jsonRowObject)) {
                // This is an error condition. However let's try and see if the JSON parses.
                jsonRowObject = logMessage;
                LOG.debug("log line didn't contain activityLog - ");
            }
        }

        EventData eventData = null;
        EventObject eventObject = null;
        try {
            eventData = gson.fromJson(jsonRowObject, EventData.class);
            eventData.setFields(logMessage);
            getNextRowHandler().processRow(eventData);
            eventObject = gson.fromJson(jsonRowObject, EventObject.class);
        } catch (Exception e) {
            LOG.error("Had a problem trying to parse JSON from the raw line {}", jsonRowObject, e);
            return;
        }
        try {
            eventObject = gson.fromJson(jsonRowObject, EventObject.class);
            eventObject.setFields(logMessage);        
            getNextRowHandler().processRow(eventObject);
        } catch (Exception e) {
            LOG.error("Had a problem trying to parse EventObject JSON from the raw line {}", jsonRowObject, e);
            return;
        }

       
    }
}
