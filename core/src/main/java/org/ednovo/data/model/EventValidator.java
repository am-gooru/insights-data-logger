/*******************************************************************************
 * EventObjectValidator.java
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
package org.ednovo.data.model;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;



public class EventValidator  {

	private static final Logger LOG = LoggerFactory.getLogger(EventValidator.class);
	private  static BaseCassandraRepoImpl baseDao;
	private static Map<String,String> acceptedFileds;
	public EventValidator() {
	this(null);
	}
	public EventValidator(Map<String ,String> object) {
		baseDao = new BaseCassandraRepoImpl();
		acceptedFileds = new HashMap<String, String>();
        Rows<String, String> rows = baseDao.readAllRows(ColumnFamilySet.EVENTFIELDS.getColumnFamily());
        for(Row<String, String> row : rows){
        	acceptedFileds.put(row.getKey(), row.getColumns().getStringValue("description", null));
        }
	
	}

	public static <T> T validateEventObject(Event event) throws JSONException  {
			Map<String,String> eventMap = JSONDeserializer.deserializeEvent(event);
			for (String fieldName : eventMap.keySet()){
				if(!acceptedFileds.containsKey(fieldName)){
					throw new NoSuchElementException("Please make sure this attribute:"+fieldName+". Or contact Adminstrator to add this attribute in event JSON list. ");
				}
			}			
		return null;
		
	}
	    
}
