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

import org.elasticsearch.client.Client;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

public class BaseDAOCassandraImpl {
	
    protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;

    private CassandraConnectionProvider connectionProvider;
    
    private Keyspace keyspace;
    
    private Keyspace awsKeyspace;
    
    private Keyspace newAwsKeyspace;
    
    private Client client;
    
    private static final Logger logger = LoggerFactory.getLogger(BaseDAOCassandraImpl.class);
    
    
    public BaseDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public void setConectionProvider(CassandraConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }
    
    public Keyspace getKeyspace() {
        if(keyspace == null && this.connectionProvider != null) {
            try {
                this.keyspace = this.connectionProvider.getKeyspace();
            } catch (IOException ex) {
                logger.info("Error while initializing keyspace{}", ex);
            }
        }
        return this.keyspace;
    }
    
    public Keyspace getAwsKeyspace() {
        if(awsKeyspace == null && this.connectionProvider != null) {
            try {
                this.awsKeyspace = this.connectionProvider.getAwsKeyspace();
            } catch (IOException ex) {
                logger.info("Error while initializing AWS keyspace{}", ex);
            }
        }
        return this.awsKeyspace;
    }
    
    public Keyspace getNewAwsKeyspace() {
        if(newAwsKeyspace == null && this.connectionProvider != null) {
            try {
                this.newAwsKeyspace = this.connectionProvider.getNewAwsKeyspace();
            } catch (IOException ex) {
                logger.info("Error while initializing New AWS keyspace{}", ex);
            }
        }
        return this.newAwsKeyspace;
    }
    
    public Client getESClient() {
        if(client == null && this.connectionProvider != null) {
            try {
                this.client = this.connectionProvider.getESClient();
            } catch (IOException ex) {
                logger.info("Error while initializing elastic search{}", ex);
            }
        }
        return this.client;
    }
}
