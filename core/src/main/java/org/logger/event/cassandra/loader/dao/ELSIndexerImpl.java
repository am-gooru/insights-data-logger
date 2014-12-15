package org.logger.event.cassandra.loader.dao;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.ednovo.data.model.EventObject;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.TypeConverter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.ESIndexices;
import org.logger.event.cassandra.loader.IndexType;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import com.google.gson.Gson;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ELSIndexerImpl extends BaseDAOCassandraImpl implements ELSIndexer,Constants {

	private static final Logger logger = LoggerFactory.getLogger(ELSIndexerImpl.class);
	
	
	private CassandraConnectionProvider connectionProvider;
	 
	private BaseCassandraRepoImpl baseDao;

	Map<String,String> fieldDataTypes = null ;
	
	Map<String,String> beFieldName = null ;

	public static  Map<String,String> cache;
    
    public static  Map<String,Object> licenseCache;
    
    public static  Map<String,Object> resourceTypesCache;
    
    public static  Map<String,Object> categoryCache;

    public static  Map<String,String> taxonomyCodeType;
    
	public ELSIndexerImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
	    this.connectionProvider = connectionProvider;
        this.baseDao = new BaseCassandraRepoImpl(this.connectionProvider);
        
        beFieldName =  new LinkedHashMap<String,String>();
        fieldDataTypes =  new LinkedHashMap<String,String>();
        Rows<String, String> fieldDescrption = baseDao.readAllRows(ColumnFamily.EVENTFIELDS.getColumnFamily(),0);
        for (Row<String, String> row : fieldDescrption) {
        	fieldDataTypes.put(row.getKey(), row.getColumns().getStringValue("description", null));
        	beFieldName.put(row.getKey(), row.getColumns().getStringValue("be_column", null));
		} 
        
        Rows<String, String> licenseRows = baseDao.readAllRows(ColumnFamily.LICENSE.getColumnFamily(),0);
        licenseCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : licenseRows) {
        	licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        Rows<String, String> resourceTypesRows = baseDao.readAllRows(ColumnFamily.RESOURCETYPES.getColumnFamily(),0);
        resourceTypesCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : resourceTypesRows) {
        	resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        Rows<String, String> categoryRows = baseDao.readAllRows(ColumnFamily.CATEGORY.getColumnFamily(),0);
        categoryCache = new LinkedHashMap<String, Object>();
        for (Row<String, String> row : categoryRows) {
        	categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
        
        taxonomyCodeType = new LinkedHashMap<String, String>();
        
        ColumnList<String> taxonomyCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), "taxonomy_code",0);
        for(int i = 0 ; i < taxonomyCodeTypeList.size() ; i++) {
        	taxonomyCodeType.put(taxonomyCodeTypeList.getColumnByIndex(i).getName(), taxonomyCodeTypeList.getColumnByIndex(i).getStringValue());
        }
        cache = new LinkedHashMap<String, String>();
        cache.put(INDEXINGVERSION, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXINGVERSION, DEFAULTCOLUMN,0).getStringValue());
	}

	public void indexActivity(String fields){
    	if(fields != null){
			try {
				JSONObject jsonField = new JSONObject(fields);
	    			if(jsonField.has("version")){
	    				EventObject eventObjects = new Gson().fromJson(fields, EventObject.class);
	    				Map<String,Object> eventMap = JSONDeserializer.deserializeEventObjectv2(eventObjects);    	
	    				
	    				eventMap.put("eventName", eventObjects.getEventName());
	    		    	eventMap.put("eventId", eventObjects.getEventId());
	    		    	eventMap.put("eventTime",String.valueOf(eventObjects.getStartTime()));
	    		    	if(eventMap.get(CONTENTGOORUOID) != null){		    		    		
	    		    		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    		    		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    		    	}
	    		    	if(eventMap.get(GOORUID) != null){  
	    		    		eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
	    		    	}
    				   if(String.valueOf(eventMap.get(CONTENTGOORUOID)) != null){
    						ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), String.valueOf(eventMap.get(CONTENTGOORUOID)),0);
    				    	if(questionList != null && questionList.size() > 0){
    				    		eventMap.put("questionCount",questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
    				    		eventMap.put("resourceCount",questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
    				    		eventMap.put("oeCount",questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
    				    		eventMap.put("mcCount",questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);
   
    				    		eventMap.put("fibCount",questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
    				    		eventMap.put("maCount",questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
    				    		eventMap.put("tfCount",questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);
   
    				    		eventMap.put("itemCount",questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L );
    				    	}
    					}
	    	    		this.saveInESIndex(eventMap,ESIndexices.EVENTLOGGERINFO.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
	    			} 
	    			else{
	    				   Iterator<?> keys = jsonField.keys();
	    				   Map<String,Object> eventMap = new HashMap<String, Object>();
	    				   while( keys.hasNext() ){
	    			            String key = (String)keys.next();
	    			            
	    			            eventMap.put(key,String.valueOf(jsonField.get(key)));
	    			            
	    			            if(key.equalsIgnoreCase("contentGooruId") || key.equalsIgnoreCase("gooruOId") || key.equalsIgnoreCase("gooruOid")){
	    			            	eventMap.put("gooruOid", String.valueOf(jsonField.get(key)));
	    			            }
	
	    			            if(key.equalsIgnoreCase("eventName") && (String.valueOf(jsonField.get(key)).equalsIgnoreCase("create-reaction"))){
	    			            	eventMap.put("eventName", "reaction.create");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-play") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-play-dots")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collections-played")
	    			            						|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("quiz-play"))){
	    			            	
	    			            	eventMap.put("eventName", "collection.play");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("signIn-google-login") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("signIn-google-home")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("anonymous-login"))){
	    			            	eventMap.put("eventName", "user.login");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("signUp-home") 
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("signUp-login"))){
	    			            	eventMap.put("eventName", "user.register");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-play") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-player")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-play-dots")
	    			            						|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-question-resource-play-dots")
	    			            							|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-oe-play-dots")
	    			            								|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-question-play-dots"))){
	    			            	eventMap.put("eventName", "collection.resource.play");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("eventName") 
	    			            		&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("resource-player") 
	    			            				|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resource-play-dots")
	    			            					|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resourceplayerstart")
	    			            						|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resourceplayerplay")
	    			            							|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resources-played")
	    			            								|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("question-oe-play-dots")
	    			            									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("question-play-dots"))){
	    			            	eventMap.put("eventName", "resource.play");
	    			            }
	    			            
	    			            if(key.equalsIgnoreCase("gooruUId") || key.equalsIgnoreCase("gooruUid")){
	    			            	eventMap.put(GOORUID, String.valueOf(jsonField.get(key)));
	    			            }
	    			            
	    			        }
	    				   if(eventMap.get(CONTENTGOORUOID) != null){
	    				   		eventMap =  this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    				   		eventMap =  this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENTGOORUOID)));
	    				   }
	    				   if(eventMap.get(GOORUID) != null ){
	    					   eventMap =   this.getUserInfo(eventMap,String.valueOf(eventMap.get(GOORUID)));
	    				   }	    	    	
	    				   
	    				   if(eventMap.get(EVENTNAME).equals(LoaderConstants.CPV1.getName()) && eventMap.get(CONTENTGOORUOID) != null){
	    						ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), String.valueOf(eventMap.get(CONTENTGOORUOID)),0);
	    				    	if(questionList != null && questionList.size() > 0){
	    				    		eventMap.put("questionCount",questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
	    				    		eventMap.put("resourceCount",questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
	    				    		eventMap.put("oeCount",questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
	    				    		eventMap.put("mcCount",questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);
	    				    		
	    				    		eventMap.put("fibCount",questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
	    				    		eventMap.put("maCount",questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
	    				    		eventMap.put("tfCount",questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);
	    				    		
	    				    		eventMap.put("itemCount",questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L );
	    				    	}
	    					}
		    	    		this.saveInESIndex(eventMap,ESIndexices.EVENTLOGGERINFO.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
	    		     }
				} catch (Exception e) {
					logger.info("Error while Migration : {} ",e);
				}
				}
		
    }

    public Map<String, Object> getUserInfo(Map<String,Object> eventMap , String gooruUId){
    	Collection<String> user = new ArrayList<String>();
    	user.add(gooruUId);
    	ColumnList<String> eventDetailsNew = baseDao.readWithKey(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), gooruUId,0);
    	//for (Row<String, String> row : eventDetailsNew) {
    		//ColumnList<String> userInfo = row.getColumns();
    	if(eventDetailsNew != null && eventDetailsNew.size() > 0){
    		for(int i = 0 ; i < eventDetailsNew.size() ; i++) {
    			String columnName = eventDetailsNew.getColumnByIndex(i).getName();
    			String value = eventDetailsNew.getColumnByIndex(i).getStringValue();
    			if(value != null){
    				eventMap.put(columnName, value);
    			}
    		}
    		}
    	//}
		return eventMap;
    }
    public Map<String,Object> getContentInfo(Map<String,Object> eventMap,String gooruOId){
    	
    	Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamily.COLLECTIONITEM.getColumnFamily(), gooruOId, 0);
    	if(!contentItems.isEmpty()){
    		eventMap.put("contentItems",contentItems);
    	}
    	ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~"+gooruOId,0);
    		if(resource != null){
    			eventMap.put("title", resource.getStringValue("title", null));
    			eventMap.put("description",resource.getStringValue("description", null));
    			eventMap.put("sharing", resource.getStringValue("sharing", null));
    			eventMap.put("category", resource.getStringValue("category", null));
    			eventMap.put("typeName", resource.getStringValue("type_name", null));
    			eventMap.put("license", resource.getStringValue("license_name", null));
    			eventMap.put("contentOrganizationId", resource.getStringValue("organization_uid", null));
    			
    			if(resource.getColumnByName("instructional_id") != null){
    				eventMap.put("instructionalId", resource.getColumnByName("instructional_id").getLongValue());
    				}
    			if(resource.getColumnByName("resource_format_id") != null){
    				eventMap.put("resourceFormatId", resource.getColumnByName("resource_format_id").getLongValue());
    			}
    				
    			if(resource.getColumnByName("type_name") != null){
					if(resourceTypesCache.containsKey(resource.getColumnByName("type_name").getStringValue())){    							
						eventMap.put("resourceTypeId", resourceTypesCache.get(resource.getColumnByName("type_name").getStringValue()));
					}
				}
				if(resource.getColumnByName("category") != null){
					if(categoryCache.containsKey(resource.getColumnByName("category").getStringValue())){    							
						eventMap.put("resourceCategoryId", categoryCache.get(resource.getColumnByName("category").getStringValue()));
					}
				}
				ColumnList<String> questionCount = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), gooruOId,0);
				if(questionCount != null && !questionCount.isEmpty()){
					long questionCounts = questionCount.getLongValue("questionCount", 0L);
					eventMap.put("questionCount", questionCounts);
					if(questionCounts > 0L){
						if(resourceTypesCache.containsKey(resource.getColumnByName("type_name").getStringValue())){    							
							eventMap.put("resourceTypeId", resourceTypesCache.get(resource.getColumnByName("type_name").getStringValue()));
						}	
					}
				}else{
					eventMap.put("questionCount",0L);
				}
    		} 
    	
		return eventMap;
    }
    
    public Map<String,Object> getTaxonomyInfo(Map<String,Object> eventMap,String gooruOid){
    	Collection<String> user = new ArrayList<String>();
    	user.add(gooruOid);
    	Map<String,String> whereColumn = new HashMap<String, String>();
    	whereColumn.put("gooru_oid", gooruOid);
    	Rows<String, String> eventDetailsNew = baseDao.readIndexedColumnList(ColumnFamily.DIMCONTENTCLASSIFICATION.getColumnFamily(), whereColumn,0);
    	Set<Long> subjectCode = new HashSet<Long>();
    	Set<Long> courseCode = new HashSet<Long>();
    	Set<Long> unitCode = new HashSet<Long>();
    	Set<Long> topicCode = new HashSet<Long>();
    	Set<Long> lessonCode = new HashSet<Long>();
    	Set<Long> conceptCode = new HashSet<Long>();
    	Set<Long> taxArray = new HashSet<Long>();

    	for (Row<String, String> row : eventDetailsNew) {
    		ColumnList<String> userInfo = row.getColumns();
    			long root = userInfo.getColumnByName("root_node_id") != null ? userInfo.getColumnByName("root_node_id").getLongValue() : null;
    			if(root == 20000L){
	    			long value = userInfo.getColumnByName("code_id") != null ?userInfo.getColumnByName("code_id").getLongValue() : null;
	    			long depth = userInfo.getColumnByName("depth") != null ?  userInfo.getColumnByName("depth").getLongValue() : null;
	    			if(value != 0L &&  depth == 1L){    				
	    				subjectCode.add(value);
	    			} 
	    			else if(depth == 2L){
	    			ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
	    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : null;
	    			if(subject != 0L)
	    				subjectCode.add(subject);
	    			if(value != 0L)
	    				courseCode.add(value);
	    			}
	    			
	    			else if(depth == 3L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : null;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : null;
		    			if(subject != 0L)
		    			subjectCode.add(subject);
		    			if(course != 0L)
	    				courseCode.add(course);
		    			if(value != 0L)
	    				unitCode.add(value);
	    			}
	    			else if(depth == 4L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : null;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : null;
		    			long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : null;
		    				if(subject != 0L)
			    			subjectCode.add(subject);	
		    				if(course != 0L)
		    				courseCode.add(course);
		    				if(unit != 0L)
		    				unitCode.add(unit);
		    				if(value != 0L)
		    				topicCode.add(value);
	    			}
	    			else if(depth == 5L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : null;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : null;
		    			long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : null;
		    			long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : null;
		    				if(subject != 0L)
			    			subjectCode.add(subject);
			    			if(course != 0L)
		    				courseCode.add(course);
		    				if(unit != 0L)
		    				unitCode.add(unit);
		    				if(topic != 0L)
		    				topicCode.add(topic);
		    				if(value != 0L)
		    				lessonCode.add(value);
	    			}
	    			else if(depth == 6L){
	    				ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value),0);
		    			long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : null;
		    			long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : null;
		    			long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : null;
		    			long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : null;
		    			long lesson = columns.getColumnByName("lesson_code_id") != null ? columns.getColumnByName("lesson_code_id").getLongValue() : null;
		    			if(subject != 0L)
		    			subjectCode.add(subject);
		    			if(course != 0L)
	    				courseCode.add(course);
	    				if(unit != 0L && unit != 0)
	    				unitCode.add(unit);
	    				if(topic != 0L)
	    				topicCode.add(topic);
	    				if(lesson != 0L)
	    				lessonCode.add(lesson);
	    				if(value != 0L)
	    				conceptCode.add(value);
	    			}
	    			else if(value != 0L){
	    				taxArray.add(value);
	    				
	    			}
    		}else{
    			long value = userInfo.getColumnByName("code_id") != null ?userInfo.getColumnByName("code_id").getLongValue() : null;
    			if(value != 0L){
    				taxArray.add(value);
    			}
    		}
    	}
    		if(subjectCode != null && !subjectCode.isEmpty())
    		eventMap.put("subject", subjectCode);
    		if(courseCode != null && !courseCode.isEmpty())
    		eventMap.put("course", courseCode);
    		if(unitCode != null && !unitCode.isEmpty())
    		eventMap.put("unit", unitCode);
    		if(topicCode != null && !topicCode.isEmpty())
    		eventMap.put("topic", topicCode);
    		if(lessonCode != null && !lessonCode.isEmpty())
    		eventMap.put("lesson", lessonCode);
    		if(conceptCode != null && !conceptCode.isEmpty())
    		eventMap.put("concept", conceptCode);
    		if(taxArray != null && !taxArray.isEmpty())
    		eventMap.put("standards", taxArray);
    	
    	return eventMap;
    }
    
    public void getResourceAndIndex(Rows<String, String> resource) throws ParseException{
    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
		SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.000");
		
		Map<String,Object> resourceMap = new LinkedHashMap<String, Object>();
		
		for(int a = 0 ; a < resource.size(); a++){
			
		ColumnList<String> columns = resource.getRowByIndex(a).getColumns();
		
		if(columns == null){
			return;
		}
		if(columns.getColumnByName("gooru_oid") != null){
			logger.info( " Migrating content : " + columns.getColumnByName("gooru_oid").getStringValue());
			Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamily.COLLECTIONITEM.getColumnFamily(), columns.getColumnByName("gooru_oid").getStringValue(), 0);
			if(!contentItems.isEmpty()){
				resourceMap.put("contentItems",contentItems);
			}
	    	
		}
		
		if(columns.getColumnByName("title") != null){
			resourceMap.put("title", columns.getColumnByName("title").getStringValue());
		}
		if(columns.getColumnByName("description") != null){
			resourceMap.put("description", columns.getColumnByName("description").getStringValue());
		}
		if(columns.getColumnByName("gooru_oid") != null){
			resourceMap.put("gooruOid", columns.getColumnByName("gooru_oid").getStringValue());
		}
		if(columns.getColumnByName("last_modified") != null){
		try{
			resourceMap.put("lastModified", formatter.parse(columns.getColumnByName("last_modified").getStringValue()));
		}catch(Exception e){
			try{
				resourceMap.put("lastModified", formatter2.parse(columns.getColumnByName("last_modified").getStringValue()));
			}catch(Exception e2){
				resourceMap.put("lastModified", formatter3.parse(columns.getColumnByName("last_modified").getStringValue()));
			}
		}
		}
		if(columns.getColumnByName("created_on") != null){
		try{
			resourceMap.put("createdOn", columns.getColumnByName("created_on") != null  ? formatter.parse(columns.getColumnByName("created_on").getStringValue()) : formatter.parse(columns.getColumnByName("last_modified").getStringValue()));
		}catch(Exception e){
			try{
				resourceMap.put("createdOn", columns.getColumnByName("created_on") != null  ? formatter2.parse(columns.getColumnByName("created_on").getStringValue()) : formatter2.parse(columns.getColumnByName("last_modified").getStringValue()));
			}catch(Exception e2){
				resourceMap.put("createdOn", columns.getColumnByName("created_on") != null  ? formatter3.parse(columns.getColumnByName("created_on").getStringValue()) : formatter3.parse(columns.getColumnByName("last_modified").getStringValue()));
			}
		}
		}
		if(columns.getColumnByName("creator_uid") != null){
			resourceMap.put("creatorUid", columns.getColumnByName("creator_uid").getStringValue());
		}
		if(columns.getColumnByName("user_uid") != null){
			resourceMap.put("userUid", columns.getColumnByName("user_uid").getStringValue());
		}
		if(columns.getColumnByName("record_source") != null){
			resourceMap.put("recordSource", columns.getColumnByName("record_source").getStringValue());
		}
		if(columns.getColumnByName("sharing") != null){
			resourceMap.put("sharing", columns.getColumnByName("sharing").getStringValue());
		}
		/*if(columns.getColumnByName("views_count") != null){
			resourceMap.put("viewsCount", columns.getColumnByName("views_count").getLongValue());
		}*/
		if(columns.getColumnByName("organization_uid") != null){
			resourceMap.put("contentOrganizationId", columns.getColumnByName("organization_uid").getStringValue());
		}
		if(columns.getColumnByName("thumbnail") != null){
			resourceMap.put("thumbnail", columns.getColumnByName("thumbnail").getStringValue());
		}
		if(columns.getColumnByName("instructional_id") != null){
			resourceMap.put("instructionalId", columns.getColumnByName("instructional_id").getLongValue());
			}
			if(columns.getColumnByName("resource_format_id") != null){
			resourceMap.put("resourceFormatId", columns.getColumnByName("resource_format_id").getLongValue());
			}
		if(columns.getColumnByName("grade") != null){
			JSONArray gradeArray = new JSONArray();
			for(String gradeId : columns.getColumnByName("grade").getStringValue().split(",")){
				gradeArray.put(gradeId);	
			}
			resourceMap.put("grade", gradeArray);
		}
		if(columns.getColumnByName("license_name") != null){
			//ColumnList<String> license = baseDao.readWithKey(ColumnFamily.LICENSE.getColumnFamily(), columns.getColumnByName("license_name").getStringValue());
			if(licenseCache.containsKey(columns.getColumnByName("license_name").getStringValue())){    							
				resourceMap.put("licenseId", licenseCache.get(columns.getColumnByName("license_name").getStringValue()));
			}
		}
		if(columns.getColumnByName("type_name") != null){
			//ColumnList<String> resourceType = baseDao.readWithKey(ColumnFamily.RESOURCETYPES.getColumnFamily(), columns.getColumnByName("type_name").getStringValue());
			if(resourceTypesCache.containsKey(columns.getColumnByName("type_name").getStringValue())){    							
				resourceMap.put("resourceTypeId", resourceTypesCache.get(columns.getColumnByName("type_name").getStringValue()));
			}
		}
		if(columns.getColumnByName("category") != null){
			//ColumnList<String> resourceType = baseDao.readWithKey(ColumnFamily.CATEGORY.getColumnFamily(), columns.getColumnByName("category").getStringValue());
			if(categoryCache.containsKey(columns.getColumnByName("category").getStringValue())){    							
				resourceMap.put("resourceCategoryId", categoryCache.get(columns.getColumnByName("category").getStringValue()));
			}
		}
		if(columns.getColumnByName("category") != null){
			resourceMap.put("category", columns.getColumnByName("category").getStringValue());
		}
		if(columns.getColumnByName("type_name") != null){
			resourceMap.put("typeName", columns.getColumnByName("type_name").getStringValue());
		}		
		if(columns.getColumnByName("gooru_oid") != null){
			ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), columns.getColumnByName("gooru_oid").getStringValue(),0);

	    	ColumnList<String> vluesList = baseDao.readWithKey(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),"all~"+columns.getColumnByName("gooru_oid").getStringValue(),0);
	    	
	    	if(vluesList != null && vluesList.size() > 0){
	    		
	    		long views = vluesList.getColumnByName("count~views") != null ?vluesList.getColumnByName("count~views").getLongValue() : 0L ;
	    		long totalTimespent = vluesList.getColumnByName("time_spent~total") != null ?vluesList.getColumnByName("time_spent~total").getLongValue() : 0L ;
	    		if(views > 0 && totalTimespent == 0L ){
	    			totalTimespent = (views * 180000);
	    			baseDao.increamentCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), "all~"+columns.getColumnByName("gooru_oid").getStringValue(), "time_spent~total", totalTimespent);
	    		}
	    		resourceMap.put("viewsCount",views);
	    		resourceMap.put("totalTimespent",totalTimespent);
	    		resourceMap.put("avgTimespent",views != 0L ? (totalTimespent/views) : 0L );
	    		
	    		long ratings = vluesList.getColumnByName("count~ratings") != null ?vluesList.getColumnByName("count~ratings").getLongValue() : 0L ;
	    		long sumOfRatings = vluesList.getColumnByName("sum~rate") != null ?vluesList.getColumnByName("sum~rate").getLongValue() : 0L ;
	    		resourceMap.put("ratingsCount",ratings);
	    		resourceMap.put("sumOfRatings",sumOfRatings);
	    		resourceMap.put("avgRating",ratings != 0L ? (sumOfRatings/ratings) : 0L );
	    		
	    		
	    		long reactions = vluesList.getColumnByName("count~reactions") != null ?vluesList.getColumnByName("count~reactions").getLongValue() : 0L ;
	    		long sumOfreactionType = vluesList.getColumnByName("sum~reactionType") != null ?vluesList.getColumnByName("sum~reactionType").getLongValue() : 0L ;
	    		resourceMap.put("reactionsCount",ratings);
	    		resourceMap.put("sumOfreactionType",sumOfreactionType);
	    		resourceMap.put("avgReaction",reactions != 0L ? (sumOfreactionType/reactions) : 0L );
	    		
	    		
	    		resourceMap.put("countOfRating5",vluesList.getColumnByName("count~5") != null ?vluesList.getColumnByName("count~5").getLongValue() : 0L );
	    		resourceMap.put("countOfRating4",vluesList.getColumnByName("count~4") != null ?vluesList.getColumnByName("count~4").getLongValue() : 0L );
	    		resourceMap.put("countOfRating3",vluesList.getColumnByName("count~3") != null ?vluesList.getColumnByName("count~3").getLongValue() : 0L );
	    		resourceMap.put("countOfRating2",vluesList.getColumnByName("count~2") != null ?vluesList.getColumnByName("count~2").getLongValue() : 0L );
	    		resourceMap.put("countOfRating1",vluesList.getColumnByName("count~1") != null ?vluesList.getColumnByName("count~1").getLongValue() : 0L );
	    		
	    		resourceMap.put("countOfICanExplain",vluesList.getColumnByName("count~i-can-explain") != null ?vluesList.getColumnByName("count~i-can-explain").getLongValue() : 0L );
	    		resourceMap.put("countOfINeedHelp",vluesList.getColumnByName("count~i-need-help") != null ?vluesList.getColumnByName("count~i-need-help").getLongValue() : 0L );
	    		resourceMap.put("countOfIDoNotUnderstand",vluesList.getColumnByName("count~i-donot-understand") != null ?vluesList.getColumnByName("count~i-donot-understand").getLongValue() : 0L );
	    		resourceMap.put("countOfMeh",vluesList.getColumnByName("count~meh") != null ?vluesList.getColumnByName("count~meh").getLongValue() : 0L );
	    		resourceMap.put("countOfICanUnderstand",vluesList.getColumnByName("count~i-can-understand") != null ?vluesList.getColumnByName("count~i-can-understand").getLongValue() : 0L );
	    		resourceMap.put("copyCount",vluesList.getColumnByName("count~copy") != null ?vluesList.getColumnByName("count~copy").getLongValue() : 0L );
	    		resourceMap.put("sharingCount",vluesList.getColumnByName("count~share") != null ?vluesList.getColumnByName("count~share").getLongValue() : 0L );
	    		resourceMap.put("commentCount",vluesList.getColumnByName("count~comment") != null ?vluesList.getColumnByName("count~comment").getLongValue() : 0L );
	    		resourceMap.put("reviewCount",vluesList.getColumnByName("count~review") != null ?vluesList.getColumnByName("count~review").getLongValue() : 0L );
	    	}
	    	
	    	if(questionList != null && questionList.size() > 0){
	    		resourceMap.put("questionCount",questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
	    		resourceMap.put("resourceCount",questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
	    		resourceMap.put("oeCount",questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
	    		resourceMap.put("mcCount",questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);
	    		
	    		resourceMap.put("fibCount",questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
	    		resourceMap.put("maCount",questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
	    		resourceMap.put("tfCount",questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);
	    		
	    		resourceMap.put("itemCount",questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L );
	    	}
		}
		if(columns.getColumnByName("user_uid") != null){
			resourceMap = this.getUserInfo(resourceMap, columns.getColumnByName("user_uid").getStringValue());
		}
		if(columns.getColumnByName("gooru_oid") != null){
			resourceMap = this.getTaxonomyInfo(resourceMap, columns.getColumnByName("gooru_oid").getStringValue());
			this.saveInESIndex(resourceMap, ESIndexices.CONTENTCATALOGINFO.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.DIMRESOURCE.getIndexType(), columns.getColumnByName("gooru_oid").getStringValue());
		}
		}
    }
    
	public void saveInESIndex(Map<String,Object> eventMap ,String indexName,String indexType,String id ) {
		XContentBuilder contentBuilder = null;
		try {
				
				contentBuilder = jsonBuilder().startObject();			
				for(Map.Entry<String, Object> entry : eventMap.entrySet()){
					String rowKey = null;  				
					if(beFieldName.containsKey(entry.getKey())){
						rowKey = beFieldName.get(entry.getKey());
					}
					if(rowKey != null && entry.getValue() != null && !entry.getValue().equals("null") && entry.getValue() != ""){	            	
		            	contentBuilder.field(rowKey, TypeConverter.stringToAny(String.valueOf(entry.getValue()),fieldDataTypes.containsKey(entry.getKey()) ? fieldDataTypes.get(entry.getKey()) : "String"));
		            }
				}
			} catch (Exception e) {
				logger.info("Indexing failed in content Builder ",e);	
			}
			
			indexingES(indexName, indexType, id, contentBuilder, 0);
	}
	
	
	public void indexingES(String indexName,String indexType,String id ,XContentBuilder contentBuilder,int retryCount){
		try{
			getESClient().prepareIndex(indexName, indexType, id).setSource(contentBuilder).execute().actionGet();
		}catch(Exception e){
			if(retryCount < 6){
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				logger.info("Retrying count: {}  ",retryCount);
				retryCount++;
    			indexingES(indexName, indexType, id, contentBuilder, retryCount);
        	}else{
        		logger.info("Indexing failed in Prod : {} ",e);
        		e.printStackTrace();
        	}
		}
		
	}


    public void getUserAndIndex(String userId) throws Exception{
    	logger.info("user id : "+ userId);
		ColumnList<String> userInfos = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), userId,0);
		
		if(userInfos != null & userInfos.size() > 0){
			
			XContentBuilder contentBuilder = jsonBuilder().startObject();
		
			if(userInfos.getColumnByName("gooru_uid") != null){
				logger.info( " Migrating User : " + userInfos.getColumnByName("gooru_uid").getStringValue()); 
				contentBuilder.field("user_uid",userInfos.getColumnByName("gooru_uid").getStringValue());
			}
			if(userInfos.getColumnByName("confirm_status") != null){
				contentBuilder.field("confirm_status",userInfos.getColumnByName("confirm_status").getLongValue());
			}
			if(userInfos.getColumnByName("registered_on") != null){
				contentBuilder.field("registered_on",TypeConverter.stringToAny(userInfos.getColumnByName("registered_on").getStringValue(), "Date"));
			}
			if(userInfos.getColumnByName("added_by_system") != null){
				contentBuilder.field("added_by_system",userInfos.getColumnByName("added_by_system").getLongValue());
			}
			if(userInfos.getColumnByName("account_created_type") != null){
				contentBuilder.field("account_created_type",userInfos.getColumnByName("account_created_type").getStringValue());
			}
			if(userInfos.getColumnByName("reference_uid") != null){
				contentBuilder.field("reference_uid",userInfos.getColumnByName("reference_uid").getStringValue());
			}
			if(userInfos.getColumnByName("email_sso") != null){
				contentBuilder.field("email_sso",userInfos.getColumnByName("email_sso").getStringValue());
			}
			if(userInfos.getColumnByName("deactivated_on") != null){
				contentBuilder.field("deactivated_on",TypeConverter.stringToAny(userInfos.getColumnByName("deactivated_on").getStringValue(), "Date"));
			}
			if(userInfos.getColumnByName("active") != null){
				contentBuilder.field("active",userInfos.getColumnByName("active").getIntegerValue());
			}
			if(userInfos.getColumnByName("last_login") != null){
				contentBuilder.field("last_login",TypeConverter.stringToAny(userInfos.getColumnByName("last_login").getStringValue(), "Date"));
			}
			if(userInfos.getColumnByName("identity_id") != null){
				contentBuilder.field("identity_id",userInfos.getColumnByName("identity_id").getIntegerValue());
			}
			if(userInfos.getColumnByName("mail_status") != null){
				contentBuilder.field("mail_status",userInfos.getColumnByName("mail_status").getLongValue());
			}
			if(userInfos.getColumnByName("idp_id") != null){
				contentBuilder.field("idp_id",userInfos.getColumnByName("idp_id").getIntegerValue());
			}
			if(userInfos.getColumnByName("state") != null){
				contentBuilder.field("state",userInfos.getColumnByName("state").getStringValue());
			}
			if(userInfos.getColumnByName("login_type") != null){
				contentBuilder.field("login_type",userInfos.getColumnByName("login_type").getStringValue());
			}
			if(userInfos.getColumnByName("user_group_uid") != null){
				contentBuilder.field("user_group_uid",userInfos.getColumnByName("user_group_uid").getStringValue());
			}
			if(userInfos.getColumnByName("primary_organization_uid") != null){
				contentBuilder.field("primary_organization_uid",userInfos.getColumnByName("primary_organization_uid").getStringValue());
			}
			if(userInfos.getColumnByName("license_version") != null){
				contentBuilder.field("license_version",userInfos.getColumnByName("license_version").getStringValue());
			}
			if(userInfos.getColumnByName("parent_id") != null){
				contentBuilder.field("parent_id",userInfos.getColumnByName("parent_id").getLongValue());
			}
			if(userInfos.getColumnByName("lastname") != null){
				contentBuilder.field("lastname",userInfos.getColumnByName("lastname").getStringValue());
			}
			if(userInfos.getColumnByName("account_type_id") != null){
				contentBuilder.field("account_type_id",userInfos.getColumnByName("account_type_id").getLongValue());
			}
			if(userInfos.getColumnByName("is_deleted") != null){
				contentBuilder.field("is_deleted",userInfos.getColumnByName("is_deleted").getIntegerValue());
			}
			if(userInfos.getColumnByName("external_id") != null){
				contentBuilder.field("external_id",userInfos.getColumnByName("external_id").getStringValue());
			}
			if(userInfos.getColumnByName("organization_uid") != null){
				contentBuilder.field("user_organization_uid",userInfos.getColumnByName("organization_uid").getStringValue());
			}
			if(userInfos.getColumnByName("import_code") != null){
				contentBuilder.field("import_code",userInfos.getColumnByName("import_code").getStringValue());
			}
			if(userInfos.getColumnByName("parent_uid") != null){
				contentBuilder.field("parent_uid",userInfos.getColumnByName("parent_uid").getStringValue());
			}
			if(userInfos.getColumnByName("security_group_uid") != null){
				contentBuilder.field("security_group_uid",userInfos.getColumnByName("security_group_uid").getStringValue());
			}
			if(userInfos.getColumnByName("username") != null){
				contentBuilder.field("username",userInfos.getColumnByName("username").getStringValue());
			}
			if(userInfos.getColumnByName("role_id") != null){
				contentBuilder.field("role_id",userInfos.getColumnByName("role_id").getLongValue());
			}
			if(userInfos.getColumnByName("firstname") != null){
				contentBuilder.field("firstname",userInfos.getColumnByName("firstname").getStringValue());
			}
			if(userInfos.getColumnByName("register_token") != null){
				contentBuilder.field("register_token",userInfos.getColumnByName("register_token").getStringValue());
			}
			if(userInfos.getColumnByName("view_flag") != null){
				contentBuilder.field("view_flag",userInfos.getColumnByName("view_flag").getLongValue());
			}
			if(userInfos.getColumnByName("account_uid") != null){
				contentBuilder.field("account_uid",userInfos.getColumnByName("account_uid").getStringValue());
			}

	    	Collection<String> user = new ArrayList<String>();
	    	user.add(userId);
	    	Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), user,0);
	    	for (Row<String, String> row : eventDetailsNew) {
	    		ColumnList<String> userInfo = row.getColumns();
	    		for(int i = 0 ; i < userInfo.size() ; i++) {
	    			String columnName = userInfo.getColumnByIndex(i).getName();
	    			String value = userInfo.getColumnByIndex(i).getStringValue();
	    			if(value != null){
	    				contentBuilder.field(columnName, value);
	    			}
	    		}
	    	}

			connectionProvider.getESClient().prepareIndex(ESIndexices.USERCATALOG.getIndex()+"_"+cache.get(INDEXINGVERSION), IndexType.DIMUSER.getIndexType(), userId).setSource(contentBuilder).execute().actionGet()			
    		;
		}else {
			throw new AccessDeniedException("Invalid Id : " + userId);
		}	
			
	}
    public void indexTaxonomy(String sourceCf, String key, String targetIndex,String targetType) throws Exception{
    	
    	for(String id : key.split(",")){
    		ColumnList<String> sourceValues = baseDao.readWithKey(sourceCf, id,0);
	    	if(sourceValues != null && sourceValues.size() > 0){
	    		XContentBuilder contentBuilder = jsonBuilder().startObject();
	            for(int i = 0 ; i < sourceValues.size() ; i++) {
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("String")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getStringValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Long")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getLongValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Integer")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getIntegerValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Double")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),sourceValues.getColumnByIndex(i).getDoubleValue());
	            	}
	            	if(taxonomyCodeType.get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Date")){
	            		contentBuilder.field(sourceValues.getColumnByIndex(i).getName(),TypeConverter.stringToAny(sourceValues.getColumnByIndex(i).getStringValue(), "Date"));
	            	}
	            }
	    		
	    		connectionProvider.getESClient().prepareIndex(targetIndex+"_"+cache.get(INDEXINGVERSION), targetType, id).setSource(contentBuilder).execute().actionGet()
	    		;
	    	}
    	}
    }
   
}
