/*******************************************************************************
 * BaseDAOCassandraImpl.java
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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.logger.event.cassandra.loader.dao;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.kafka.log.writer.producer.KafkaLogProducer;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.datasource.infra.CassandraClient;

import com.datastax.driver.core.Session;

public abstract class BaseDAOCassandraImpl {
	
	public String getAnalyticsKeyspaceName() {
		return CassandraClient.getAnalyticsKeyspace();
	}
	public String getEventKeyspaceName() {
		return CassandraClient.getEventKeyspace();
	}
	
	public KafkaLogProducer getKafkaLogProducer(){
		return CassandraClient.getKafkaLogProducer();
	}
	
	public Session getAnalyticsCassSession() {
		return CassandraClient.getAnalyticsCassandraSession();
	} 
	
	public Session getEventCassSession() {
		return CassandraClient.getEventCassandraSession();
	}
	
	public String setNAIfNull(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null && StringUtils.isNotBlank((String)eventMap.get(fieldName))){
			return (String) eventMap.get(fieldName);
		}
		return Constants.NA;
	}
	public String setNullIfEmpty(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null && StringUtils.isNotBlank((String)eventMap.get(fieldName))){
			return (String) eventMap.get(fieldName);
		}
		return null;
	}
	public long setLongZeroIfNull(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null){
			return ((Number) eventMap.get(fieldName)).longValue();
		}
		return 0L;
	}
	public int setIntegerZeroIfNull(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null){
			return ((Number) eventMap.get(fieldName)).intValue();
		}
		return 0;
	}
	public String appendTildaSeperator(String... columns) {
		StringBuilder columnKey = new StringBuilder();
		for (String column : columns) {
			if (StringUtils.isNotBlank(column)) {
				columnKey.append(columnKey.length() > 0 ? Constants.SEPERATOR : Constants.EMPTY);
				columnKey.append(column);
			}
		}
		return columnKey.toString();

	}
}
