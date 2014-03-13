/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   CollectionItemDAOImpl.java
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

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class CollectionItemDAOImpl extends BaseDAOCassandraImpl implements CollectionItemDAO{

	 private static final Logger logger = LoggerFactory.getLogger(CollectionItemDAOImpl.class);

	 private final ColumnFamily<String, String> collectionItemCF;
	 
	 private static final String CF_COLLECTION_ITEM_NAME = "collection_item";
	    
	public CollectionItemDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		collectionItemCF = new ColumnFamily<String, String>(
				CF_COLLECTION_ITEM_NAME, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
	}
	
public String getParentId(String Key){

	Rows<String, String> collectionItem = null;
	String parentId = null;
	try {
		collectionItem = getKeyspace().prepareQuery(collectionItemCF)
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		 	.searchWithIndex().setRowLimit(1)
			.addExpression()
			.whereColumn("resource_gooru_oid")
			.equals()
			.value(Key).execute().getResult();
	} catch (ConnectionException e) {
		
		logger.info("Error while retieveing data : {}" ,e);
	}
	if(collectionItem == null){
		return "NA";
	}
	for(Row<String, String> collectionItems : collectionItem){
		parentId =  collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue() == null ? "NA" : collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue();
	 }
		return parentId;
	} 

}
