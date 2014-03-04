/*******************************************************************************
 * DimDateDAOCassandraImpl.java
 * core
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

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class DimDateDAOCassandraImpl extends BaseDAOCassandraImpl implements DimDateDAO {
	

    private static final Logger logger = LoggerFactory.getLogger(DimDateDAOCassandraImpl.class);
    private final ColumnFamily<String, String> dimDateCF;
    private static final String CF_DIM_DATE = "dim_date";


	public DimDateDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		dimDateCF = new ColumnFamily<String, String>(
				CF_DIM_DATE, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
	}
	
	public String getDateId(String currentDate) {

		String dateId = null ;
		
		Rows<String, String> dateDetail = null;
		try {
			dateDetail = getKeyspace().prepareQuery(dimDateCF)
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.searchWithIndex().setRowLimit(1)
					.addExpression()
					.whereColumn("date")
					.equals().value(currentDate).execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		 for(Row<String, String> dateIds : dateDetail){
			
			 dateId = dateIds.getKey().toString();
		 }
		 
		return dateId;
		
	}

}
