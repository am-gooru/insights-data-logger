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
    
	String visitor = "visitor";
	
	String visitorType = "loggedInUser";
	
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
    	    
    	    if(lastCount != 0L){
    	    	long difference = (thisCount*100)/lastCount;
    	    	baseDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), entry.getKey()+SEPERATOR+entry.getValue(), DIFF+SEPERATOR+eventMap.get(EVENTNAME), String.valueOf(difference), m);
    	    }
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
			logger.info("dashboardKeys : {} ",dashboardKeys);
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
	
	private boolean isRowAvailable(String cfName ,String key,String  columnName){
		ColumnList<String>  result = baseDao.readWithKey(cfName, key);
		
		if (result != null && !result.isEmpty() && result.getColumnByName(columnName) != null) {
				return true;
    	}		
		return false;
	}

	@Async
	public void addApplicationSession(Map<String,String> eventMap){
		
		String dateKey = hourlyDateFormatter.format(new Date()).toString();
		
		if(eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")){
			visitorType = "anonymousUser";
		}
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		if(!this.isRowAvailable(ColumnFamily.MICROAGGREGATION.getColumnFamily(),dateKey, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID))){
			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),visitor, COUNT+SEPERATOR+visitorType, 1, m);
		}	
		baseDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(),dateKey, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), secondDateFormatter.format(new Date()).toString(), m);
		
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
		String lastUpdated = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(),"last~updated~session","constant_value").getStringValue();
		String currentHour = hourlyDateFormatter.format(new Date()).toString();

		if(lastUpdated == null || lastUpdated.equals(currentHour)){
			this.updateExpiredToken(currentHour);
		}else{
			ColumnList<String> tokenList = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), lastUpdated);
		 	for(int i = 0 ; i < tokenList.size() ; i++) {
		 		String column = tokenList.getColumnByIndex(i).getName();
		 		String value = tokenList.getColumnByIndex(i).getStringValue();
		 		String[] parts = column.split("~");
		 		
		 		if(parts[1].equalsIgnoreCase("ANONYMOUS")){
					visitorType = "anonymousUser";
				}
		 		if(!value.equalsIgnoreCase("expired") && this.isRowAvailable(ColumnFamily.MICROAGGREGATION.getColumnFamily(),currentHour, column)){
		 			baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),visitor, COUNT+SEPERATOR+visitorType, -1, m);
		 		}else{
		 			baseDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(),currentHour, column,value, m);
		 		}
		 	}
		}
		try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}

	public void updateExpiredToken(String timeLine) throws ParseException {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		ColumnList<String> tokenList = baseDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), timeLine);
	 	for(int i = 0 ; i < tokenList.size() ; i++) {
	 		String column = tokenList.getColumnByIndex(i).getName();
	 		String value = tokenList.getColumnByIndex(i).getStringValue();
	 		String[] parts = column.split("~");
	 		
	 		if(parts[1].equalsIgnoreCase("ANONYMOUS")){
				visitorType = "anonymousUser";
			}
	 		if(!value.equalsIgnoreCase("expired")){
	 			Date valueInDate = secondDateFormatter.parse(value);
	 			int diffInMinutes = (int)( (new Date().getTime() - valueInDate.getTime() ) / ((60 * 1000) % 60)) ;
	 			if(diffInMinutes > 30){
	 				baseDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(),visitor, visitorType, "expired", m);
	 				baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(),visitor, COUNT+SEPERATOR+visitorType, -1, m);
	 			}
	 		}
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
