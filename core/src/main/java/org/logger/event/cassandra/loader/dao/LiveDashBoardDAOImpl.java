package org.logger.event.cassandra.loader.dao;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.ednovo.data.geo.location.GeoLocation;
import org.ednovo.data.model.GeoData;
import org.ednovo.data.model.TypeConverter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.ESIndexices;
import org.logger.event.cassandra.loader.IndexType;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.restlet.data.Form;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.maxmind.geoip2.model.CityResponse;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class LiveDashBoardDAOImpl  extends BaseDAOCassandraImpl implements LiveDashBoardDAO,Constants{

	private static final Logger logger = LoggerFactory.getLogger(LiveDashBoardDAOImpl.class);

    private CassandraConnectionProvider connectionProvider;
    
    private MicroAggregatorDAOmpl microAggregatorDAOmpl;
    
    private SimpleDateFormat secondDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");

    private SimpleDateFormat minDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
    
    private SimpleDateFormat hourlyDateFormatter = new SimpleDateFormat("yyyyMMddkk");

    private SimpleDateFormat customDateFormatter;
    
    private BaseCassandraRepoImpl baseDao;

    String dashboardKeys = null;
    
    String browserDetails = null;
    
    String customEventsConfig = null;
    
    Map<String,String> fieldDataTypes = null ;
    
    Map<String,String> beFieldName = null ;
    
    ColumnList<String> eventKeys = null;
    	
    Collection<String> esEventFields = null;
    
    public LiveDashBoardDAOImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        this.microAggregatorDAOmpl = new MicroAggregatorDAOmpl(this.connectionProvider);
        this.baseDao = new BaseCassandraRepoImpl(this.connectionProvider);
        dashboardKeys = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"dashboard~keys",DEFAULTCOLUMN).getStringValue();
        browserDetails = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"available~browsers",DEFAULTCOLUMN).getStringValue();
        customEventsConfig = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"custom~events",DEFAULTCOLUMN).getStringValue();
        /*String[] esFields = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"es~fields",DEFAULTCOLUMN).getStringValue().split(",");
        esEventFields = Arrays.asList(esFields);*/
        fieldDataTypes =  new LinkedHashMap<String,String>();
        beFieldName =  new LinkedHashMap<String,String>();
        //Rows<String, String> fieldTypes = baseDao.readWithKeyList(ColumnFamily.EVENTFIELDS.getColumnFamily(), esEventFields);
        Rows<String, String> fieldDescrption = baseDao.readAllRows(ColumnFamily.EVENTFIELDS.getColumnFamily());
        for (Row<String, String> row : fieldDescrption) {
        	fieldDataTypes.put(row.getKey(), row.getColumns().getStringValue("description", null));
        	beFieldName.put(row.getKey(), row.getColumns().getStringValue("be_column", null));
		}        
    }
    
    public void clearCache(){  
    	dashboardKeys = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"dashboard~keys",DEFAULTCOLUMN).getStringValue();
	    browserDetails = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"available~browsers",DEFAULTCOLUMN).getStringValue();
	    /*String[] esFields = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"es~fields",DEFAULTCOLUMN).getStringValue().split(",");
	    esEventFields = Arrays.asList(esFields);*/
	    fieldDataTypes =  new LinkedHashMap<String,String>();
        beFieldName =  new LinkedHashMap<String,String>();
        //Rows<String, String> fieldTypes = baseDao.readWithKeyList(ColumnFamily.EVENTFIELDS.getColumnFamily(), esEventFields);
        Rows<String, String> fieldDescrption = baseDao.readAllRows(ColumnFamily.EVENTFIELDS.getColumnFamily());
        for (Row<String, String> row : fieldDescrption) {
        	fieldDataTypes.put(row.getKey(), row.getColumns().getStringValue("description", null));
        	beFieldName.put(row.getKey(), row.getColumns().getStringValue("be_column", null));
		}   
    }
    
    
    public void callCountersV2(Map<String,String> eventMap) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	if((eventMap.containsKey(EVENTNAME))) {
            eventKeys = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),eventMap.get("eventName"));
            for(int i=0 ; i < eventKeys.size() ; i++ ){
            	String columnName = eventKeys.getColumnByIndex(i).getName();
            	String columnValue = eventKeys.getColumnByIndex(i).getStringValue();
        		String key = this.formOrginalKey(columnName, eventMap);
        		for(String value : columnValue.split(",")){
            		String orginalColumn = this.formOrginalKey(value, eventMap);
	            		if(!(eventMap.containsKey(TYPE) && eventMap.get(TYPE).equalsIgnoreCase(STOP) && orginalColumn.startsWith(COUNT+SEPERATOR))) {
	            			if(!orginalColumn.startsWith(TIMESPENT+SEPERATOR) && !orginalColumn.startsWith("sum"+SEPERATOR)){
	            				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),key, orginalColumn, 1L, m);
	            			}else if(orginalColumn.startsWith(TIMESPENT+SEPERATOR)){
	            				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),key, orginalColumn, Long.valueOf(String.valueOf(eventMap.get(TOTALTIMEINMS))),m);
	            			}else if(orginalColumn.startsWith("sum"+SEPERATOR)){
	            				String[] rowKey = orginalColumn.split("~");
	            				logger.info("rowKey[1]" + rowKey[1]);
	            				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),key, orginalColumn, rowKey[1].equalsIgnoreCase("reactionType") ? DataUtils.formatReactionString(eventMap.get(rowKey[1])) : Long.valueOf(String.valueOf(eventMap.get(rowKey[1].trim()) == null ? "0":eventMap.get(rowKey[1].trim()))),m);
	            				
	            			}
	            		} 
            		}
                	try {
                        m.execute();
                    } catch (ConnectionException e) {
                        logger.info("updateCounter => Error while inserting to cassandra via callCountersV2 {} ", e);
                    }
            	
            }
    	}
    }
    
    @Async
    public void callCountersV2Custom(Map<String,String> eventMap) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	if((eventMap.containsKey(EVENTNAME))) {
            eventKeys = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),eventMap.get("eventName"));
            for(int i=0 ; i < eventKeys.size() ; i++ ){
            	String columnName = eventKeys.getColumnByIndex(i).getName();
            	String columnValue = eventKeys.getColumnByIndex(i).getStringValue();
        		String key = this.formOrginalKey(columnName, eventMap);
        		for(String value : columnValue.split(",")){
            		String orginalColumn = this.formOrginalKey(value, eventMap);
	            		if(!(eventMap.containsKey(TYPE) && eventMap.get(TYPE).equalsIgnoreCase(STOP) && orginalColumn.startsWith(COUNT+SEPERATOR))) {
	            			if(!orginalColumn.startsWith(TIMESPENT+SEPERATOR) && !orginalColumn.startsWith("sum"+SEPERATOR)){
	            				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARDTEST.getColumnFamily(),key, orginalColumn, 1L, m);
	            			}else if(orginalColumn.startsWith(TIMESPENT+SEPERATOR)){
	            				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARDTEST.getColumnFamily(),key, orginalColumn, Long.valueOf(String.valueOf(eventMap.get(TOTALTIMEINMS))),m);
	            			}else if(orginalColumn.startsWith("sum"+SEPERATOR)){
	            				logger.info("orginalColumn" + orginalColumn); 
	            				String[] rowKey = orginalColumn.split("~");
	            				logger.info("rowKey[1]" + rowKey[1]);
	            				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARDTEST.getColumnFamily(),key, orginalColumn, rowKey[1].equalsIgnoreCase("reactionType") ? DataUtils.formatReactionString(eventMap.get(rowKey[1])) : Long.valueOf(String.valueOf(eventMap.get(rowKey[1]))),m);
	            				
	            			}
	            		} 
            		}
                	try {
                        m.execute();
                    } catch (ConnectionException e) {
                        logger.info("updateCounter => Error while inserting to cassandra via callCountersV2 {} ", e);
                    }
            	
            }
    	}
    }
    
    public String getBrowser(String userAgent) {
    	String browser = "";
    	if(!userAgent.isEmpty() && userAgent != null) {
            	for(String browserList : browserDetails.split(",")) {
            		if(userAgent.indexOf(browserList)!= -1) {
            	         browser = browserList;
            		}
            	}
    	}
    	return browser;
    }
    
    @Async
    public void pushEventForAtmosphere(String atmosphereEndPoint, Map<String,String> eventMap) throws JSONException{
   		
    	JSONObject filtersObj = new JSONObject();
    	filtersObj.put("eventName", eventMap.get("eventName")+","+customEventsConfig);

		JSONObject mainObj = new JSONObject();
		mainObj.put("filters", filtersObj);		

    	ClientResource clientResource = null;
    	clientResource = new ClientResource(atmosphereEndPoint+"/push/message");
    	Form forms = new Form();
		forms.add("data", mainObj.toString());
		clientResource.post(forms.getWebRepresentation());
       logger.info("atmos status : {} ",clientResource.getStatus());
    }

    @Async
    public void pushEventForAtmosphereProgress(String atmosphereEndPoint, Map<String,String> eventMap) throws JSONException{
   		
    	JSONObject filtersObj = new JSONObject();
    	JSONObject paginateObj = new JSONObject();
    	Collection<String> fields = new ArrayList<String>();
    	fields.add("timeSpent");
    	fields.add("avgTimeSpent");
    	fields.add("resourceGooruOId");
    	fields.add("OE");
    	fields.add("questionType");
    	fields.add("category");
    	fields.add("gooruUId");
    	fields.add("userName");
    	fields.add("userData");
    	fields.add("metaData");
    	fields.add("title");
    	fields.add("reaction");
    	fields.add("description");
    	fields.add("options");
    	fields.add("skip");
    	filtersObj.put("session","FS");
    	paginateObj.put("sortBy","itemSequence");
    	paginateObj.put("sortOrder","ASC");
    	
    	List<String> classpage = microAggregatorDAOmpl.getPathWaysFromClass(eventMap);
    	
    	for(String classId : classpage){
	    	filtersObj.put("classId",classId);
			JSONObject mainObj = new JSONObject();
			mainObj.put("filters", filtersObj);
			mainObj.put("paginate", paginateObj);
			mainObj.put("fields", fields);
	
	    	ClientResource clientResource = null;
	    	clientResource = new ClientResource(atmosphereEndPoint+"/classpage/users/usage");
	    	Form forms = new Form();
			forms.add("data", mainObj.toString());
			forms.add("collectionId", eventMap.get(PARENTGOORUOID));
			clientResource.post(forms.getWebRepresentation());
    	}
       
    }
    
    public void findDifferenceInCount(Map<String,String> eventMap) throws ParseException{
    	
    	Map<String,String>  aggregator = this.generateKeyValues(eventMap);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	
    	for (Map.Entry<String, String> entry : aggregator.entrySet()) {
    		Column<String> thisCountList = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), entry.getKey(), COUNT+SEPERATOR+eventMap.get(EVENTNAME));
    		Column<String> lastCountList = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), entry.getValue(), COUNT+SEPERATOR+eventMap.get(EVENTNAME)); 
    	    long thisCount = thisCountList != null ? thisCountList.getLongValue() : 0L;
    	    long lastCount = lastCountList != null ? lastCountList.getLongValue() : 0L;
    	    long difference = 100L;
    	    long findDifference = 0L;
    	    if(lastCount != 0L){
    	    	if(thisCount > lastCount) {
    	    		findDifference = thisCount - lastCount;
    	    		difference = (findDifference/lastCount)*100;
    	    	} else {
    	    		difference = 0L;
    	    	}
    	    }
    	    baseDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), entry.getKey()+SEPERATOR+entry.getValue(), DIFF+SEPERATOR+eventMap.get(EVENTNAME), String.valueOf(difference), m);
    	}    	
    	try {
            m.execute();
        } catch (Exception e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
    }
    
    @Async
	public Map<String,String> generateKeyValues(Map<String,String> eventMap) throws ParseException{
		Map<String,String> returnDate = new LinkedHashMap<String, String>();
		if(dashboardKeys != null){
			for(String key : dashboardKeys.split(",")){
				String rowKey = null;
				if(!key.equalsIgnoreCase("all")) {
					customDateFormatter = new SimpleDateFormat(key);
					Date eventDateTime = new Date(Long.valueOf(eventMap.get(STARTTIME)));
					rowKey = customDateFormatter.format(eventDateTime);
					Date lastDate = customDateFormatter.parse(rowKey);
					Date rowValues = new Date(lastDate.getTime() - 2);
					returnDate.put(customDateFormatter.format(lastDate), customDateFormatter.format(rowValues));
					if(eventMap.containsKey(CLIENTSOURCE)) {
						returnDate.put(customDateFormatter.format(lastDate)+SEPERATOR+eventMap.get(CLIENTSOURCE), customDateFormatter.format(rowValues)+SEPERATOR+eventMap.get(CLIENTSOURCE));
					}
					if(eventMap.get(ORGANIZATIONUID) != null && !eventMap.get(ORGANIZATIONUID).isEmpty()){
						returnDate.put(customDateFormatter.format(lastDate)+SEPERATOR+eventMap.get(ORGANIZATIONUID), customDateFormatter.format(rowValues)+SEPERATOR+eventMap.get(ORGANIZATIONUID));
					}
					if(eventMap.get(GOORUID) != null && !eventMap.get(GOORUID).isEmpty()){
						returnDate.put(customDateFormatter.format(lastDate)+SEPERATOR+eventMap.get(GOORUID), customDateFormatter.format(rowValues)+SEPERATOR+eventMap.get(GOORUID));
					}
				} 
			}
		}
		return returnDate; 
	}
    
    private void saveGeoLocation(GeoData geoData,Map<String,String> eventMap){
    	ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    	String json = null ;
    	String rowKey = "geo~locations";
    	String columnName = null;
    	if(geoData.getLatitude() != null && geoData.getLongitude() != null){    		
    		 columnName = geoData.getLatitude()+"x"+geoData.getLongitude();
    	}
		try {
			 json = ow.writeValueAsString(geoData);
		} catch (JsonProcessingException e) {
			logger.info("Exception while converting Object as JSON : {}",e);
		}
		if(columnName != null){
			baseDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), rowKey, columnName, json);
		}
		baseDao.increamentCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), columnName, COUNT+SEPERATOR+eventMap.get(EVENTNAME), 1);
    }	
    
	public void addApplicationSession(Map<String,String> eventMap){
		
		int expireTime = 3600;
		int contentExpireTime = 600;
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if(eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")){
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ANONYMOUSSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),expireTime, m);
		}else{
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),USERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),expireTime, m);
		}
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ALLUSERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),expireTime, m);

		if(eventMap.get(EVENTNAME).equalsIgnoreCase("logOut") || eventMap.get(EVENTNAME).equalsIgnoreCase("user.logout")){
				if(eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")){
					baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ALLUSERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID));
				}else{
					baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), USERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID));
				}
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ALLUSERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID));
		}
			
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){				
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVECOLLECTIONPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
			}else{
				baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ACTIVECOLLECTIONPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),contentExpireTime, m);
			}
		}
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RP1.getName())){
			if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){				
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVERESOURCEPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
			}else{
				baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ACTIVERESOURCEPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),contentExpireTime, m);
			}
		}
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
			if(eventMap.get(TYPE).equalsIgnoreCase(STOP)){				
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVECOLLECTIONRESOURCEPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
			}else{
				baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ACTIVECOLLECTIONRESOURCEPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),contentExpireTime, m);
			}
		}
		
		try {
            m.execute();
        } catch (Exception e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}
	
	public void addContentForPostViews(Map<String,String> eventMap){
		String dateKey = minDateFormatter.format(new Date()).toString();
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		
		logger.info("Key- view : {} ",VIEWS+SEPERATOR+dateKey);
		
		baseDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(),VIEWS+SEPERATOR+dateKey, eventMap.get(CONTENTGOORUOID), eventMap.get(CONTENTGOORUOID), m);
		
		try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}
	
	public void watchApplicationSession() throws ParseException{
	
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		long allUserSessionCounts = 0L;
		long anonymousSessionCounts = 0L;
		long loggedUserSessionCounts = 0L;
		
		long allCollectionPlayCounts = 0L;
		long allResourcePlayCounts = 0L;
		long allCollectionResourcePlayCounts = 0L;
		
		try{
			Column<String> allUserSessionCount = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), ACTIVESESSION,COUNT+SEPERATOR+ALLUSERSESSION);
			Column<String> anonymousSessionCount = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), ACTIVESESSION,COUNT+SEPERATOR+ANONYMOUSSESSION);
			Column<String> loggedUserSessionCount = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), ACTIVESESSION,COUNT+SEPERATOR+USERSESSION);
			
			ColumnList<String> allUserSession = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ALLUSERSESSION);
			ColumnList<String> anonymousSession = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ANONYMOUSSESSION);
			ColumnList<String> loggedUserSession = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), USERSESSION);
			
			Column<String> allCollectionPlayCount = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), ACTIVEPLAYS,COUNT+SEPERATOR+ACTIVECOLLECTIONPLAYS);
			Column<String> allResourcePlayCount = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), ACTIVEPLAYS,COUNT+SEPERATOR+ACTIVERESOURCEPLAYS);
			Column<String> allCollectionResourcePlayCount = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), ACTIVEPLAYS,COUNT+SEPERATOR+ACTIVECOLLECTIONRESOURCEPLAYS);
			
			ColumnList<String> allCollectionPlay = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVECOLLECTIONPLAYS);
			ColumnList<String> allResourcePlay = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVERESOURCEPLAYS);
			ColumnList<String> allCollectionResourcePlay = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVECOLLECTIONRESOURCEPLAYS);
			
			if(allUserSession != null)
			allUserSessionCounts = allUserSession.size();
			
			if(anonymousSession != null)
			anonymousSessionCounts = anonymousSession.size();
			
			if(loggedUserSession != null)
			loggedUserSessionCounts = loggedUserSession.size();
			
			if(allResourcePlay != null)
			allResourcePlayCounts = allResourcePlay.size();
				
			if(allCollectionPlay != null)
			allCollectionPlayCounts = allCollectionPlay.size();
				
			if(allCollectionResourcePlay != null)
			allCollectionResourcePlayCounts = allCollectionResourcePlay.size();
				
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVESESSION, COUNT+SEPERATOR+ALLUSERSESSION, (allUserSessionCounts - allUserSessionCount.getLongValue()), m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVESESSION, COUNT+SEPERATOR+ANONYMOUSSESSION, (anonymousSessionCounts  - anonymousSessionCount.getLongValue()), m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVESESSION, COUNT+SEPERATOR+USERSESSION, (loggedUserSessionCounts - loggedUserSessionCount.getLongValue()), m);
			
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVEPLAYS, COUNT+SEPERATOR+ACTIVECOLLECTIONPLAYS, (allCollectionPlayCounts - allCollectionPlayCount.getLongValue()), m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVEPLAYS, COUNT+SEPERATOR+ACTIVERESOURCEPLAYS, (allResourcePlayCounts  - allResourcePlayCount.getLongValue()), m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVEPLAYS, COUNT+SEPERATOR+ACTIVECOLLECTIONRESOURCEPLAYS, (allCollectionResourcePlayCounts - allCollectionResourcePlayCount.getLongValue()), m);
			
		}catch(Exception e){
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVESESSION, COUNT+SEPERATOR+ALLUSERSESSION, 0, m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVESESSION, COUNT+SEPERATOR+ANONYMOUSSESSION,0, m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVESESSION, COUNT+SEPERATOR+USERSESSION, 0, m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVEPLAYS, COUNT+SEPERATOR+ACTIVECOLLECTIONPLAYS, 0, m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVEPLAYS, COUNT+SEPERATOR+ACTIVERESOURCEPLAYS,0, m);
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),ACTIVEPLAYS, COUNT+SEPERATOR+ACTIVECOLLECTIONRESOURCEPLAYS, 0, m);
			logger.info("Exception in watching session : {}",e);
		}
		
		try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}


/*	C: defines => Constant
	D: defines => Date format lookup
	E: defines => eventMap param lookup*/
	public String formOrginalKey(String value,Map<String,String> eventMap){
		Date eventDateTime = new Date();
		String key = "";
		for(String splittedKey : value.split("~")){	
			String[]  subKey = null;
			if(splittedKey.startsWith("C:")){
				subKey = splittedKey.split(":");
				key += "~"+subKey[1];
			}
			if(splittedKey.startsWith("D:")){
				subKey = splittedKey.split(":");
				customDateFormatter = new SimpleDateFormat(subKey[1]);
				if(eventMap != null){
					 eventDateTime = new Date(Long.valueOf(eventMap.get("startTime")));					
				}
				key += "~"+customDateFormatter.format(eventDateTime).toString();
			}
			if(splittedKey.startsWith("E:") && eventMap != null){
				subKey = splittedKey.split(":");
				if(splittedKey.contains(USERAGENT)) {
					key += "~"+this.getBrowser(eventMap.get(USERAGENT));
				} else {
					key += "~"+(eventMap.get(subKey[1]) != null ? eventMap.get(subKey[1]).toLowerCase() : subKey[1]);
				}
			}
			if(!splittedKey.startsWith("C:") && !splittedKey.startsWith("D:") && !splittedKey.startsWith("E:")){
				try {
					throw new AccessDeniedException("Unsupported key format : " + splittedKey);
				} catch (AccessDeniedException e) {
					e.printStackTrace();
				}
			}
		}
		return key != null ? key.substring(1).trim():null;
	}

	@Async
	public void saveActivityInESIndex(Map<String,Object> eventMap ,String indexName,String indexType,String id ) {
		try {
			XContentBuilder contentBuilder = jsonBuilder().startObject();
			String rowKey = null;  
			for(Map.Entry<String, Object> entry : eventMap.entrySet()){
				if(beFieldName.containsKey(entry.getKey()) && rowKey != null){
					rowKey = beFieldName.get(entry.getKey());
				}
	            if(rowKey != null && fieldDataTypes.containsKey(entry.getKey())&& entry.getValue() != null){	            	
	            	contentBuilder.field(rowKey, TypeConverter.stringToAny(String.valueOf(entry.getValue()), fieldDataTypes.get(entry.getKey())));
	            }
			}
				getESClient().prepareIndex(indexName, indexType, id)
				.setSource(contentBuilder)
				.execute()
				.actionGet()
				;
			} catch (Exception e) {
				logger.info("Indexing failed",e);
			}
		
	}

	@Async
	public void saveInESIndex(Map<String,Object> eventMap ,String indexName,String indexType,String id ) {
		try {
			XContentBuilder contentBuilder = jsonBuilder().startObject();
			String rowKey = null;  
			for(Map.Entry<String, Object> entry : eventMap.entrySet()){
				
				if(beFieldName.containsKey(entry.getKey()) && rowKey != null){
					rowKey = beFieldName.get(entry.getKey());
				}
				
				if(rowKey != null && entry.getValue() != null){	            	
	            	contentBuilder.field(rowKey, TypeConverter.stringToAny(String.valueOf(entry.getValue()),fieldDataTypes.containsKey(entry.getKey()) ? fieldDataTypes.get(entry.getKey()) : "String"));
	            }else if(rowKey != null){
	            	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("String")){        		
	            		contentBuilder.field(rowKey, String.valueOf(entry.getValue()));
	            	}
	            	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Integer")){        		
	            		contentBuilder.field(rowKey, Integer.valueOf(""+entry.getValue()));
	            	}
	            	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Long")){        		
	            		contentBuilder.field(rowKey, Long.valueOf(""+entry.getValue()));
	            	}
	            	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Boolean")){        		
	            		contentBuilder.field(rowKey, Boolean.valueOf(""+entry.getValue()));
	            	}
	            	else{        		
	            		contentBuilder.field(rowKey, entry.getValue());
	            	}
	            }
			}
				getESClient().prepareIndex(indexName, indexType, id)
				.setSource(contentBuilder)
				.execute()
				.actionGet()
				;
			} catch (Exception e) {
				logger.info("Indexing failed",e);
			}
		
	}
	
	@Async
	public void saveInStaging(Map<String,Object> eventMap) {
		try {
			MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL); 
			ColumnListMutation<String> m = mutationBatch.withRow(baseDao.accessColumnFamily(ColumnFamily.STAGING.getColumnFamily()), eventMap.get("eventId").toString());
			String rowKey = null;
			for(Map.Entry<String, Object> entry : eventMap.entrySet()){
				
				if(beFieldName.containsKey(entry.getKey()) && rowKey != null){
					rowKey = beFieldName.get(entry.getKey());
				}else{
					rowKey = entry.getKey();
				}
				
				String typeToChange =  fieldDataTypes.containsKey(entry.getKey()) ? fieldDataTypes.get(entry.getKey()) : "String";		       
				baseDao.generateNonCounter(rowKey, TypeConverter.stringToAny(String.valueOf(entry.getValue()),typeToChange), m);
		    }
			
			mutationBatch.execute();
		} catch (Exception e) {
			logger.info("Staging data failed",e);
		}
	}
	@Async
	public void saveGeoLocations(Map<String,String> eventMap) throws IOException{
    	
		if(eventMap.containsKey("userIp") && eventMap.get("userIp") != null && !eventMap.get("userIp").isEmpty()){
			
			GeoData geoData = new GeoData();
			
			GeoLocation geo = new GeoLocation();
			
			CityResponse res = geo.getGeoResponse(eventMap.get("userIp"));			

			if(res != null && res.getCountry().getName() != null){
				geoData.setCountry(res.getCountry().getName());
				eventMap.put("country", res.getCountry().getName());
			}
			if(res != null && res.getCity().getName() != null){
				geoData.setCity(res.getCity().getName());
				eventMap.put("city", res.getCity().getName());
			}
			if(res != null && res.getLocation().getLatitude() != null){
				geoData.setLatitude(res.getLocation().getLatitude());
			}
			if(res != null && res.getLocation().getLongitude() != null){
				geoData.setLongitude(res.getLocation().getLongitude());
			}
			if(res != null && res.getMostSpecificSubdivision().getName() != null){
				geoData.setState(res.getMostSpecificSubdivision().getName());
				eventMap.put("state", res.getMostSpecificSubdivision().getName());
			}
			
			if(geoData.getLatitude() != null && geoData.getLongitude() != null){
				this.saveGeoLocation(geoData,eventMap);
			}			
		}
    }
}
