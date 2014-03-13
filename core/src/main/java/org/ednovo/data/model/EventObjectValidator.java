/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   EventObjectValidator.java
 *   event-api-stable-1.2
 *   
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *  
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *  
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.ednovo.data.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.dao.FieldsDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class EventObjectValidator  {

	private static final Logger logger = LoggerFactory.getLogger(EventObjectValidator.class);
	private  static FieldsDAOImpl fieldsDAOImpl;
	private  CassandraConnectionProvider connectionProvider;
	private static Map<String,String> acceptedFileds;
	public EventObjectValidator() {
		this(null);
    }
	public EventObjectValidator(Map<String ,String> object) {
		init(object);
	}
	private void init(Map<String ,String> object) {
		this.setConnectionProvider(new CassandraConnectionProvider());
        this.getConnectionProvider().init(null);
        fieldsDAOImpl = new FieldsDAOImpl(getConnectionProvider());
        acceptedFileds = fieldsDAOImpl.readAllFields();
	}

	public static <T> T validateEventObject(EventObject eventObject) throws JSONException  {
			Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObject);
			for (String fieldName : eventMap.keySet()){
				if(!acceptedFileds.containsKey(fieldName)){
					throw new NoSuchElementException("Pleae make sure this attribute:"+fieldName+". Or contact Adminstrator to add this attribute in event JSON list. ");
				}
			}			
		return null;
		
	}
	 public CassandraConnectionProvider getConnectionProvider() {
	    	return connectionProvider;
	    }
	    
	    /**
	     * @param connectionProvider the connectionProvider to set
	     */
	    public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
	    	this.connectionProvider = connectionProvider;
	    }
	    
		public static void main (String a[]) { 
	    	SimpleDateFormat minuteDateFormatter;

	    	minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

	    	Date eventDateTime = new Date();
	    	String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
	    	
	    	System.out.println(eventRowKey);
	    	
	    }
}
