/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   DimTimeDAOCassandraImpl.java
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
package org.logger.event.cassandra.loader.dao;

import java.util.ArrayList;
import java.util.List;

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class DimTimeDAOCassandraImpl extends BaseDAOCassandraImpl implements DimTimeDAO{

	    private static final Logger logger = LoggerFactory.getLogger(DimTimeDAOCassandraImpl.class);
	    private final ColumnFamily<String, String> dimTimeCF;
	    private static final String CF_DIM_TIME = "dim_time";

	public DimTimeDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		dimTimeCF = new ColumnFamily<String, String>(
				CF_DIM_TIME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
	}
	
	public Rows<String, String> getTimeId(String currentHour , String currentMinute){
		
		Rows<String, String> hourDetail = null;
		try {
			hourDetail = getKeyspace().prepareQuery(dimTimeCF)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.searchWithIndex().setRowLimit(1)
					.addExpression()
					.whereColumn("hour")
					.equals().value(currentHour)
					.addExpression().whereColumn("minute")
					.equals().value(currentMinute)
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		 
		return hourDetail;
		
	}
	
}
