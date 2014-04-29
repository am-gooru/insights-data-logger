/*******************************************************************************
 * CollectionItemDAOImpl.java
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

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class DimExtractedCodeDAOImpl extends BaseDAOCassandraImpl implements DimExtractedCodeDAO,Constants{

	 private static final Logger logger = LoggerFactory.getLogger(DimExtractedCodeDAOImpl.class);

	 private final ColumnFamily<String, String> extractedCodeCF;
	 
	 private static final String CF_EXTRACTED_CODE = "extracted_code";
	    
	public DimExtractedCodeDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		extractedCodeCF = new ColumnFamily<String, String>(
				CF_EXTRACTED_CODE, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
	}
	
	public Map<Long,String> getParentId(String[] codeIds){
	
		Rows<String, String> taxonomiesInfo = null;
		Map<Long,String> taxonomies = new HashMap<Long, String>();
		
		try {
			taxonomiesInfo = getKeyspace().prepareQuery(extractedCodeCF)
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.getKeySlice(codeIds)
			 	.execute()
			 	.getResult()
			 	;
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		for (Row<String, String> row : taxonomiesInfo) {
				if(row.getColumns().getLongValue("subject_code_id", null) != null){
					taxonomies.put(row.getColumns().getLongValue("subject_code_id", null), row.getColumns().getStringValue("subject", null));
				}
				if(row.getColumns().getLongValue("course_code_id", null) != null){
					taxonomies.put(row.getColumns().getLongValue("course_code_id", null), row.getColumns().getStringValue("course", null));
				}
			}
		return taxonomies; 
		}
}
