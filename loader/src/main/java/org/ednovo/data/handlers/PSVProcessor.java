/*******************************************************************************
 * PSVProcessor.java
 * loader
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

import org.ednovo.data.model.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PSVProcessor extends BaseDataProcessor implements DataProcessor {
	static final Logger LOG = LoggerFactory.getLogger(PSVProcessor.class);

	@Override
	public void handleRow(Object row) throws Exception{

		String psvMessage = "";
		if (row instanceof String) {
			psvMessage = (String) row;
		}
//		LOG.info("entered in to handlepsvMessage");

		EventData eventData = new EventData();
		String columnName[] = psvMessage.split("~;");

		eventData.setUserIp(columnName[0]);
		eventData.setStartTime(Long.parseLong(columnName[1]));
		eventData.setEndTime(Long.parseLong(columnName[2]));
//		eventData.setEventName(columnName[3]);
		eventData.setSessionToken(columnName[4]);
		eventData.setContext(columnName[5]);
		eventData.setUserId(columnName[6]);
		
		eventData.setOrganizationUid("4261b188-ccae-11e1-adfb-5404a609bd14");

		getNextRowHandler().processRow(eventData);
	}
}
