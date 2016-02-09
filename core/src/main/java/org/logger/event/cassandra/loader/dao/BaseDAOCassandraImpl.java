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

import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.UserCo;
import org.elasticsearch.client.Client;
import org.logger.event.datasource.infra.CassandraClient;
import org.logger.event.datasource.infra.ELSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;

@Component
public class BaseDAOCassandraImpl {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseDAOCassandraImpl.class);
	
	protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	@Autowired
	private CassandraClient cassandraClient;
	
	public Keyspace getKeyspace() {
		try {
			return cassandraClient.getKeyspace();
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
}
