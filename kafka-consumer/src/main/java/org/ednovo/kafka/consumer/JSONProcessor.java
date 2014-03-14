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
			LOG.error("Had a problem trying to parse JSON from the raw line {}", jsonRowObject, e);
			return;
		}
		
		if(row == null) {
			LOG.error("The row was null. This is invalid");
			return;
		}
		
		// Override and set fields to be the original log message / JSON. 
        eventData.setFields(jsonRowObject);
        
		getNextRowHandler().processRow(eventData);
		long partEndTime = System.currentTimeMillis();
		LOG.trace("Cassandra update: {} ms : Total : {} ms " , (partEndTime - startTime),  (partEndTime  - firstStartTime));
	}

	public static void main(String args[])
	{
		Gson gson = new Gson();
		EventData eventData = gson.fromJson("{\"userIp\":\"38.64.65.241\",\"organizationUid\":\"4261739e-ccae-11e1-adfb-5404a609bd14\",\"resourceInstanceId\":\"2b60dff5-fe36-4e2a-aadc-c07189421461\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31\",\"sessionActivityItemId\":\"null\",\"sessionActivityId\":null,\"endTime\":1367366400798,\"type\":\"start\",\"startTime\":1367366400779,\"sessionToken\":\"156e533d-fffa-4581-abe2-4f932e7919ee\",\"contentGooruId\":\"38ceafa3-de9f-4158-98ec-19b8724fd188\",\"eventId\":\"7147dc48-c799-7712-feff-a4049689e2c8\",\"parentGooruId\":\"03bfd74b-3171-48b1-8ff4-3a4b870a4424\",\"parentEventId\":\"e5c8c0db-b566-d588-3d07-53dcc0c66848\",\"userId\":11878,\"context\":\"/activity/log/7147dc48-c799-7712-feff-a4049689e2c8/start\",\"eventName\":\"collection-play\",\"imeiCode\":\"null\"}", EventData.class);
		System.out.println("Output json\n" + eventData.getEventName()+ "\n");
	}
	
	
}
