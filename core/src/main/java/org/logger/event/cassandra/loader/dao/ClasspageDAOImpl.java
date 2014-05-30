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

public class ClasspageDAOImpl extends BaseDAOCassandraImpl implements ClasspageDAO,Constants{

	 private static final Logger logger = LoggerFactory.getLogger(ClasspageDAOImpl.class);

	 private final ColumnFamily<String, String> classpageCF;
	 
	 private static final String CF_CLASSPAGE = "classpage";
	    
	 private CassandraConnectionProvider connectionProvider;
	 
	public ClasspageDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
		classpageCF = new ColumnFamily<String, String>(
				CF_CLASSPAGE, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
	}
	
public List<String> getParentId(String Key){

	Rows<String, String> collectionItem = null;
	List<String> classPages = new ArrayList<String>();
	String parentId = null;
	try {
		collectionItem = getKeyspace().prepareQuery(classpageCF)
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		 	.searchWithIndex().setRowLimit(1)
			.addExpression()
			.whereColumn("resource_gooru_oid")
			.equals()
			.value(Key).execute().getResult();
	} catch (ConnectionException e) {
		
		logger.info("Error while retieveing data : {}" ,e);
	}
	if(collectionItem != null){
		for(Row<String, String> collectionItems : collectionItem){
			parentId =  collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue() == null ? "NA" : collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue();
			classPages.add(parentId);
		 }
	}
	return classPages; 
	} 

	public void updateClasspage(Map<String ,String> eventMap){

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        
		logger.info("Classpage KEY : {} ", eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GROUPUID)+SEPERATOR+eventMap.get(GOORUID));
		//for create classpage group owner should be 1.
		int isGroupOwner = 0;
		
         m.withRow(classpageCF, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GROUPUID)+SEPERATOR+eventMap.get(GOORUID))
        .putColumnIfNotNull(USER_GROUP_UID,eventMap.get(GROUPUID))
        .putColumnIfNotNull(CLASSPAGE_GOORU_OID,eventMap.get(CONTENTGOORUOID))
        .putColumnIfNotNull("is_group_owner",isGroupOwner)
        .putColumnIfNotNull(USERID,eventMap.get(GOORUID))
        .putColumnIfNotNull(CLASSPAGE_CODE,eventMap.get(CLASSCODE))
        .putColumnIfNotNull(USER_GROUP_CODE,eventMap.get(CONTENT_GOORU_OID))
        .putColumnIfNotNull(ORGANIZATION_UID,eventMap.get(ORGANIZATIONUID))
        
        ;
        
         try{
          	m.execute();
          } catch (ConnectionException e) {
          	logger.info("Error while inserting to cassandra - JSON - ", e);
          }
	}

	public boolean getClassPageOwnerInfo(String key ,String classPageGooruOid){

		Rows<String, String>  result = null;
		boolean isOwner;
    	try {
    		 result = getKeyspace().prepareQuery(classpageCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
    		 	.searchWithIndex()
				.addExpression()
				.whereColumn("gooru_uid")
				.equals().value(key).execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data: {}" ,e);
		}
		
    	if (result != null && !result.isEmpty()) {
      		 for(Row<String, String> column : result){	
      			 String classId = column.getColumns().getStringValue("classpage_gooru_oid", null);
      			 int ownerStatus = column.getColumns().getIntegerValue("is_group_owner", null);
    			 if(classId != null && classPageGooruOid != null && classPageGooruOid.equalsIgnoreCase(classId) && ownerStatus == 1 ){
    				 return true;
    			 }
    			 
    		 }
    	}
    	return false;
	
	}

	public boolean isUserPartOfClass(String key ,String classPageGooruOid){

		Rows<String, String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(classpageCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
    		 	.searchWithIndex()
				.addExpression()
				.whereColumn("classpage_gooru_oid")
				.equals().value(classPageGooruOid)
				.addExpression()
				.whereColumn("gooru_uid")
				.equals().value(key)
				.execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data: {}" ,e);
		}
		
    	if (result != null && !result.isEmpty()) {
    		return true;
    	}
    	return false;
	
	}
}
