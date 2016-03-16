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

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.UserCo;
import org.elasticsearch.client.Client;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.datasource.infra.CassandraClient;
import org.logger.event.datasource.infra.ELSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;

public abstract class BaseDAOCassandraImpl {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseDAOCassandraImpl.class);
	
	protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	public static final PreparedStatement INSERT_USER_SESSION = getCassSession().prepare(Constants.INSERT_USER_SESSION);
	
	public static final PreparedStatement INSERT_USER_LAST_SESSION = getCassSession().prepare(Constants.INSERT_USER_LAST_SESSION);
	
	public static final PreparedStatement INSERT_USER_SESSION_ACTIVITY = getCassSession().prepare(Constants.INSERT_USER_SESSION_ACTIVITY);	
	
	public static final PreparedStatement INSERT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare(Constants.INSERT_STUDENTS_CLASS_ACTIVITY);
	
	public static final PreparedStatement INSERT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.INSERT_CONTENT_TAXONOMY_ACTIVITY);	
	
	public static final PreparedStatement INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY);
	
	public static final PreparedStatement INSERT_USER_LOCATION = getCassSession().prepare(Constants.INSERT_USER_LOCATION);
	
	public static final PreparedStatement UPDATE_PEER_COUNT = getCassSession().prepare(Constants.UPDATE_PEER_COUNT);
	
	public static final PreparedStatement UPDATE_PEER_DETAILS_ON_START = getCassSession().prepare(Constants.UPDATE_PEER_DETAILS_ON_START);
	
	public static final PreparedStatement UPDATE_PEER_DETAILS_ON_STOP = getCassSession().prepare(Constants.UPDATE_PEER_DETAILS_ON_STOP);
	
	public static final PreparedStatement SELECT_USER_SESSION_ACTIVITY = getCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY);
	
	public static final PreparedStatement SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID = getCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID);
	
	public static final PreparedStatement SELECT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare(Constants.SELECT_STUDENTS_CLASS_ACTIVITY);
	
	public static final PreparedStatement UPDATE_REACTION = getCassSession().prepare(Constants.UPDATE_REACTION);
	
	public static final PreparedStatement UPDATE_SESSION_SCORE = getCassSession().prepare(Constants.UPDATE_SESSION_SCORE);
	
	public static final PreparedStatement SELECT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_CLASS_ACTIVITY_DATACUBE);
	
	public static final PreparedStatement SELECT_ALL_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_ALL_CLASS_ACTIVITY_DATACUBE);
	
	public static final PreparedStatement INSERT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.INSERT_CLASS_ACTIVITY_DATACUBE);
	
	public static final PreparedStatement SELECT_TAXONOMY_PARENT_NODE = getCassSession().prepare(Constants.SELECT_TAXONOMY_PARENT_NODE);
	
	public static final PreparedStatement SELECT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.SELECT_CONTENT_TAXONOMY_ACTIVITY);
	
	public static final PreparedStatement SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY);
	
	public static final PreparedStatement SELECT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_TAXONOMY_ACTIVITY_DATACUBE);
	
	public static final PreparedStatement INSERT_USER_QUESTION_GRADE = getCassSession().prepare(Constants.INSERT_USER_QUESTION_GRADE);
	
	public static final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_SESSION = getCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_SESSION);
	
	public static final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_QUESTION = getCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_QUESTION);
	
	public static final PreparedStatement INSERT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.INSERT_TAXONOMY_ACTIVITY_DATACUBE);

	
	public Keyspace getKeyspace() {
		try {
			return CassandraClient.getKeyspace();
		} catch (IOException e) {
			LOG.error("Exception : ",e);
		}
		return null;
	}

	public Client getESClient() {
		try {
			return ELSClient.getESClient();
		} catch (IOException e) {
			LOG.error("Exception : ",e);
		}
		return null;
	}

	public EntityManager<ResourceCo, String> getResourceEntityPersister() {
		try {
			return CassandraClient.getResourceEntityPersister();
		} catch (IOException e) {
			LOG.error("Exception : ",e);
		}
		return null;
	}

	public EntityManager<UserCo, String> getUserEntityPersister() {
		try {
			return CassandraClient.getUserEntityPersister();
		} catch (IOException e) {
			LOG.error("Exception : ",e);
		}
		return null;
	}
	
	public String getLogKeyspaceName() {
		return CassandraClient.getLogKeyspaceName();
	}

	public Session getCassSession() {
		return CassandraClient.getCassSession();
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
