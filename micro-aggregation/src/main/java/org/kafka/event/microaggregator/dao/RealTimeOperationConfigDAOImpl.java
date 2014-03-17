/*******************************************************************************
 * RealTimeOperationConfigDAOImpl.java
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
package org.kafka.event.microaggregator.dao;

import java.util.HashMap;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class RealTimeOperationConfigDAOImpl extends BaseDAOCassandraImpl implements RealTimeOperationConfigDAO{


    private static final Logger logger = LoggerFactory.getLogger(RealTimeOperationConfigDAOImpl.class);
    private final ColumnFamily<String, String> realTimeConfigCF;
    private static final String CF_REAL_TIME_CONFIG = "real_time_operation_config";
   

	public RealTimeOperationConfigDAOImpl(
			CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		
		realTimeConfigCF = new ColumnFamily<String, String>(
					CF_REAL_TIME_CONFIG, // Column Family Name
	                StringSerializer.get(), // Key Serializer
	                StringSerializer.get()); // Column Serializer
	}

	public HashMap<String,String> getOperators(){
		Rows<String, String> operators = null;
		HashMap<String,String> operatorsObject = new HashMap<String,String>();
		try {
			operators = getKeyspace().prepareQuery(realTimeConfigCF)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getAllRows()
					.withColumnRange(new RangeBuilder().setMaxSize(10).build())
			        .setExceptionCallback(new ExceptionCallback() {
			             @Override
			             public boolean onException(ConnectionException e) {
			                 try {
			                     Thread.sleep(1000);
			                 } catch (InterruptedException e1) {
			                 }
			                 return true;
			             }})
			        .execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		for (Row<String, String> row : operators) {
				operatorsObject.put(row.getKey(), row.getColumns().getStringValue("aggregator_json", null));
			}
		return operatorsObject;
	}
	
	public void addAggregators(String key,String json,String updateBy){
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		
		m.withRow(realTimeConfigCF, key)
		.putColumnIfNotNull("updated_by", updateBy,null)
        .putColumnIfNotNull("aggregator_json", json, null);
		
		try{
	     	m.execute();
	     } catch (ConnectionException e) {
	     	logger.info("Error while inserting to cassandra - JSON - ", e);
	     }
	}
}
