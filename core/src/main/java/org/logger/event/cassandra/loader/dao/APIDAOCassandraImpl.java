/*******************************************************************************
 * APIDAOCassandraImpl.java
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
package org.logger.event.cassandra.loader.dao;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class APIDAOCassandraImpl extends BaseDAOCassandraImpl {

    private ColumnFamily<String, String> apiKeyCF;
    private static final String CF_API_KEY_NAME = "app_api_key";
    private static final Logger logger = LoggerFactory.getLogger(APIDAOCassandraImpl.class);

    public APIDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        apiKeyCF = new ColumnFamily<String, String>(CF_API_KEY_NAME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer        
    }

    /**
     * @param appName,appKey
     * @throws ConnectionException
     *             if host is unavailable 
     */
    public void save(String appName, String apiKey) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        m.withRow(apiKeyCF, apiKey).putColumn("app_name", appName, null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while generating api key cassandra", e);
        }
    }

    public String read(String apiKey) {
        String appName = null;
        try {
            Column<String> result = getKeyspace().prepareQuery(apiKeyCF)
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKey(apiKey)
                    .getColumn("app_name")
                    .execute().getResult();

            appName = result.getStringValue();

            return appName;

        } catch (ConnectionException e) {
            logger.info("Error while fetching api key from cassandra", e);
        }
        return appName;
    }
   public ColumnList<String> readApiData(String key){
    	
    	ColumnList<String> apiKeyValues = null;
    	try {
			 apiKeyValues = getKeyspace().prepareQuery(apiKeyCF)
			 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 .getKey(key)
			 .execute().getResult();
    	} catch (ConnectionException e) {
			
			logger.info("Error while fetching api data " ,e);
		}
    	
    	return apiKeyValues;
    }
}
