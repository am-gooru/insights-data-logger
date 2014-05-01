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

public class CollectionItemDAOImpl extends BaseDAOCassandraImpl implements CollectionItemDAO,Constants{

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
	
public List<String> getParentId(String Key){

	Rows<String, String> collectionItem = null;
	List<String> classPages = new ArrayList<String>();
	String parentId = null;
	try {
		collectionItem = getKeyspace().prepareQuery(collectionItemCF)
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		 	.searchWithIndex()
			.addExpression()
			.whereColumn("resource_gooru_oid")
			.equals()
			.value(Key).execute().getResult();
	} catch (ConnectionException e) {
		
		logger.info("Error while retieveing data : {}" ,e);
	}
	if(collectionItem != null){
		for(Row<String, String> collectionItems : collectionItem){
			parentId =  collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue();
			if(parentId != null){
				classPages.add(parentId);
			}
		 }
	}
	return classPages; 
	} 

	public void updateCollectionItem(Map<String ,String> eventMap){
	
	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    
	 m.withRow(collectionItemCF, eventMap.get(COLLECTIONITEMID))
     .putColumnIfNotNull(CONTENT_ID,eventMap.get(CONTENTID))
     .putColumnIfNotNull(PARENT_CONTENT_ID,eventMap.get(PARENTCONTENTID))
     .putColumnIfNotNull(RESOURCE_GOORU_OID,eventMap.get(CONTENT_GOORU_OID))
     .putColumnIfNotNull(COLLECTION_GOORU_OID,eventMap.get(PARENT_GOORU_OID))
     .putColumnIfNotNull(ITEM_SEQUENCE,eventMap.get(ITEMSEQUENCE))
     .putColumnIfNotNull(ORGANIZATION_UID,eventMap.get(ORGANIZATIONUID))
    ;
    
}
}
