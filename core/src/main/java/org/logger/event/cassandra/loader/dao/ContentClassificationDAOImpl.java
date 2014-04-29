/*******************************************************************************
 * CollectionIte`mDAOImpl.java
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class ContentClassificationDAOImpl extends BaseDAOCassandraImpl implements ContentClassificationDAO,Constants{

	 private static final Logger logger = LoggerFactory.getLogger(ContentClassificationDAOImpl.class);

	 private final ColumnFamily<String, String> contentClassificationCF;
	 
	 private static final String CF_CONTENT_CLASSIFICATION = "dim_content_classification";
	    
	public ContentClassificationDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		contentClassificationCF = new ColumnFamily<String, String>(
				CF_CONTENT_CLASSIFICATION, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
	}
	
public Long[] getParentId(String Key){

	Rows<String, String> resourceMappings = null;
	Long[] codeIds = null ;
	Long codeId = 0L;
		try {
			resourceMappings = getKeyspace().prepareQuery(contentClassificationCF)
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.searchWithIndex().setRowLimit(1)
				.addExpression()
				.whereColumn("gooru_oid")
				.equals()
				.value(Key).execute().getResult();
			} catch (ConnectionException e) {
			logger.info("Error while retieveing data : {}" ,e);
		}
		
			if(resourceMappings != null){
			codeIds = new Long[resourceMappings.size()];
			for(Row<String, String> collectionItems : resourceMappings){ 
				int i = 1;
				codeId =  collectionItems.getColumns().getColumnByName("code_id").getLongValue() == 0L ? 0L : collectionItems.getColumns().getColumnByName("code_id").getLongValue();
				codeIds[i] = codeId;
				i++;
			 }
		}
		return codeIds; 
	} 
}
