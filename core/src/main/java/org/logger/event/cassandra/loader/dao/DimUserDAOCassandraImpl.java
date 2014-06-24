/*******************************************************************************
 * DimUserDAOCassandraImpl.java
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

import java.util.HashMap;
import java.util.Map;

import org.ednovo.data.model.EventData;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class DimUserDAOCassandraImpl extends BaseDAOCassandraImpl implements DimUserDAO {

	private static final Logger logger = LoggerFactory.getLogger(DimUserDAOCassandraImpl.class);
    private final ColumnFamily<String, String> dimUserCF;
    private static final String CF_DIM_DATE = "dim_user";

    
	public DimUserDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		dimUserCF = new ColumnFamily<String, String>(
				CF_DIM_DATE, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer

	}
	
	public String getUserUid(String userId) throws ConnectionException{

		ColumnList<String> existingEventRecord = getKeyspace().prepareQuery(dimUserCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(userId).execute().getResult();
	
		return existingEventRecord.getStringValue("gooru_uid", null);
		
	}
	@Async
	public String getUserName(String userUid) throws ConnectionException{
		String userName = null;
		Rows<String, String> user = getKeyspace().prepareQuery(dimUserCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		.searchWithIndex()
		.addExpression()
		.whereColumn("gooru_uid").equals().value(userUid)
		.execute().getResult();
		for (Row<String, String> userRow : user) {
			Map<String, Object> columnMap = new HashMap<String, Object>();
			for (Column<String> column : userRow.getColumns()) {
				if (column.getName().equalsIgnoreCase("username")) {
					userName =  column.getStringValue();
				}
			}
		}
		return userName;
	}

	public String getOrganizationUid(String userUid) throws ConnectionException{
		String organizationUid = null;
		Rows<String, String> user = getKeyspace().prepareQuery(dimUserCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		.searchWithIndex()
		.addExpression()
		.whereColumn("gooru_uid").equals().value(userUid)
		.execute().getResult();
		for (Row<String, String> userRow : user) {
			Map<String, Object> columnMap = new HashMap<String, Object>();
			for (Column<String> column : userRow.getColumns()) {
				if (column.getName().equalsIgnoreCase("organization_uid")) {
					organizationUid =  column.getStringValue();
				}
			}
		}
		return organizationUid;
	}
	
}
