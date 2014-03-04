/*******************************************************************************
 * JSONProcessor.java
 * kafka-consumer
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
package org.ednovo.kafka.consumer;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.EventData;

import com.google.gson.Gson;

public class JSONProcessor extends BaseDataProcessor implements DataProcessor {
    private Gson gson;

    public JSONProcessor() {
    	this.gson = new Gson();
	}
    
	@Override
	public void handleRow(Object row) throws Exception {
		String jsonRowObject = (String)row;
		
		if(StringUtils.isEmpty(jsonRowObject)) {
			return;
		}
		
		long startTime = System.currentTimeMillis(), firstStartTime = System.currentTimeMillis();
		EventData eventData = null;
		try {
			eventData = gson.fromJson(jsonRowObject, EventData.class);
			eventData.setFields(jsonRowObject);
			
		} catch (Exception e) {
			logger.error("Had a problem trying to parse JSON from the raw line {}", jsonRowObject, e);
			return;
		}
		
		if(row == null) {
			logger.error("The row was null. This is invalid");
			return;
		}
		
		// Override and set fields to be the original log message / JSON. 
        eventData.setFields(jsonRowObject);
        
		getNextRowHandler().processRow(eventData);
		long partEndTime = System.currentTimeMillis();
		logger.trace("Cassandra update: {} ms : Total : {} ms " , (partEndTime - startTime),  (partEndTime  - firstStartTime));
	}
	
}
