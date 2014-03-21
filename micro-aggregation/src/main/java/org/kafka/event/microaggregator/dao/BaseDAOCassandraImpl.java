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
package org.kafka.event.microaggregator.dao;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

class BaseDAOCassandraImpl {
    protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_LOCAL_QUORUM;

    private CassandraConnectionProvider connectionProvider;
    private Keyspace keyspace;

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
                Logger.getLogger(BaseDAOCassandraImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return this.keyspace;
    }
}
