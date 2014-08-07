package org.logger.event.cassandra.loader.dao;

import java.nio.file.AccessDeniedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.ednovo.data.model.GeoData;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.restlet.data.Form;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

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
    
    ColumnList<String> eventKeys = null;
    	
    public LiveDashBoardDAOImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        this.microAggregatorDAOmpl = new MicroAggregatorDAOmpl(this.connectionProvider);
        this.baseDao = new BaseCassandraRepoImpl(this.connectionProvider);
        dashboardKeys = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"dashboard~keys","constant_value").getStringValue();
        browserDetails = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"available~browsers","constant_value").getStringValue();
    }
    
    public void clearCache(){
    	dashboardKeys = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"dashboard~keys","constant_value").getStringValue();
    }
    
    @Async
    public void callCountersV2(Map<String,String> eventMap) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	if((eventMap.containsKey(EVENTNAME))) {
            eventKeys = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),eventMap.get("eventName")+SEPERATOR+"columnkey");
            for(int i=0 ; i < eventKeys.size() ; i++ ){
            	String columnName = eventKeys.getColumnByIndex(i).getName();
            	String columnValue = eventKeys.getColumnByIndex(i).getStringValue();
        		String key = this.formOrginalKey(columnName, eventMap);
        		
        		for(String value : columnValue.split(",")){
            		String orginalColumn = this.formOrginalKey(value, eventMap);
	            		if(!(eventMap.containsKey(TYPE) && eventMap.get(TYPE).equalsIgnoreCase(STOP) && orginalColumn.startsWith(COUNT+SEPERATOR))) {
	            			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),key, orginalColumn, orginalColumn.startsWith(TIMESPENT+SEPERATOR) ? Long.valueOf(String.valueOf(eventMap.get(TOTALTIMEINMS))) : 1L, m);
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
    	filtersObj.put("eventName", eventMap.get("eventName"));

		JSONObject mainObj = new JSONObject();
		mainObj.put("filters", filtersObj);		

    	ClientResource clientResource = null;
    	clientResource = new ClientResource(atmosphereEndPoint+"/push/message");
    	Form forms = new Form();
		forms.add("data", mainObj.toString());
		clientResource.post(forms.getWebRepresentation());
       
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
    	
    	List<String> classpage = microAggregatorDAOmpl.getClassPages(eventMap);
    	
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
    
    @Async
    public void findDifferenceInCount(Map<String,String> eventMap) throws ParseException{
    	
    	Map<String,String>  aggregator = this.generateKeyValues(eventMap);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	
    	for (Map.Entry<String, String> entry : aggregator.entrySet()) {
    		Column<String> thisCountList = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), entry.getKey(), COUNT+SEPERATOR+eventMap.get(EVENTNAME));
    		Column<String> lastCountList = baseDao.readWithKeyColumn(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), entry.getValue(), COUNT+SEPERATOR+eventMap.get(EVENTNAME)); 
    	    long thisCount = thisCountList != null ? thisCountList.getLongValue() : 0L;
    	    long lastCount = lastCountList != null ? lastCountList.getLongValue() : 0L;
    	    long difference = 100L;
    	    if(lastCount != 0L){
    	    	 difference = (thisCount*100)/lastCount;
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
    
    @Async
    public void saveGeoLocation(GeoData geoData){
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
    }
	
	@Async
	public void addApplicationSession(Map<String,String> eventMap){
		
		int expireTime = 3600;
		int contentExpireTime = 600;
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase("logOut") || eventMap.get(EVENTNAME).equalsIgnoreCase("user.logout")){
			expireTime = 1;
		}
		
		if((eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RP1.getName()) ||  eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName()) || eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())) &&  eventMap.get(TYPE).equalsIgnoreCase(STOP)){
			contentExpireTime = 1;
		}
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if(eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")){
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ANONYMOUSSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),expireTime, m);
		}else{
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),USERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),expireTime, m);
		}
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ALLUSERSESSION, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),expireTime, m);
		
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ACTIVECOLLECTIONPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),contentExpireTime, m);
		}
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.RP1.getName())){
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ACTIVERESOURCEPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),contentExpireTime, m);
		}
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())){
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(),ACTIVECOLLECTIONRESOURCEPLAYS, eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(),contentExpireTime, m);
		}
		
		try {
            m.execute();
        } catch (Exception e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}
	
	@Async
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
				if(eventMap.containsKey(USERAGENT) && eventMap.get(USERAGENT) != null) {
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
}
