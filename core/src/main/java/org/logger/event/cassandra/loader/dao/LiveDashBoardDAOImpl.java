package org.logger.event.cassandra.loader.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class LiveDashBoardDAOImpl  extends BaseDAOCassandraImpl implements LiveDashBoardDAO,Constants{

	private static final Logger logger = LoggerFactory.getLogger(LiveDashBoardDAOImpl.class);

    private final ColumnFamily<String, String> microAggregator;
    
    private static final String CF_MICRO_AGGREGATOR = "micro_aggregation";

    private final ColumnFamily<String, String> liveDashboard;
    
    private static final String CF_LIVE_DASHBOARD = "live_dashboard";

    private CassandraConnectionProvider connectionProvider;
    
    private CollectionItemDAOImpl collectionItem;
    
    private EventDetailDAOCassandraImpl eventDetailDao; 
    
    private DimResourceDAOImpl dimResource;
    
    private DimUserDAOCassandraImpl dimUser;
    
    private ClasspageDAOImpl classpage;
    
    private CollectionDAOImpl collection;
    
    private SimpleDateFormat customDateFormatter;

    private JobConfigSettingsDAOCassandraImpl configSettings;
    
    String dashboardKeys = null;
    
    public LiveDashBoardDAOImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        this.connectionProvider = connectionProvider;
        liveDashboard = new ColumnFamily<String, String>(
        		CF_LIVE_DASHBOARD, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
        microAggregator = new ColumnFamily<String, String>(
        		CF_MICRO_AGGREGATOR, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer

        this.collectionItem = new CollectionItemDAOImpl(this.connectionProvider);
        this.eventDetailDao = new EventDetailDAOCassandraImpl(this.connectionProvider);
        this.dimResource = new DimResourceDAOImpl(this.connectionProvider);
        this.classpage = new ClasspageDAOImpl(this.connectionProvider);
        this.collection = new CollectionDAOImpl(this.connectionProvider);
        this.dimUser = new DimUserDAOCassandraImpl(this.connectionProvider);
        this.configSettings = new JobConfigSettingsDAOCassandraImpl(this.connectionProvider);    
        this.customDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
        dashboardKeys = configSettings.getConstants("dashboard~keys","constant_value");

    }

    @Async
    public void callCounters(Map<String,String> eventMap) throws JSONException, ParseException, ConnectionException {
		if(eventMap.containsKey(EVENTNAME) && eventMap.containsKey(GOORUID)) {
			String gooruUId = eventMap.get(GOORUID);
			String eventName = eventMap.get(EVENTNAME);
			String createdOn = eventMap.get(STARTTIME);
			String userName = dimUser.getUserName(gooruUId);
			String organizationUId = eventMap.get(DEFAULT_ORGANIZATION_UID);
			if(eventMap.get(ORGANIZATIONUID) != null && !eventMap.get(ORGANIZATIONUID).isEmpty()) {
					organizationUId = eventMap.get(ORGANIZATIONUID);
			}
			
			boolean isRowAvailable =  this.isRowAvailable(METRICS, eventName);
			
			logger.info("Is row available : {} ", isRowAvailable);
			
			if(!isRowAvailable){
				this.addRowColumn(METRICS, eventName, String.valueOf(TimeUUIDUtils.getUniqueTimeUUIDinMillis()));
			}
			List<String> keys = this.generateYMWDKey(eventMap.get(STARTTIME));
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for(String key : keys) {
				if(eventMap.get(TYPE).equals(eventMap.get(START))) {
					// Increment the view using start events
					generateCounter(key,eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,eventName,1, m);
				}
					// Calculate the timestamp using stop events
					generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName,Long.valueOf(eventMap.get(TIMEINMS)), m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,Long.valueOf(eventMap.get(TIMEINMS)), m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,Long.valueOf(eventMap.get(TIMEINMS)), m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,Long.valueOf(eventMap.get(TIMEINMS)), m);						

				generateAggregator(key, eventName+SEPERATOR+LASTACCESSED, createdOn, m);
				generateAggregator(key, eventName+SEPERATOR+LASTACCESSEDUSERUID, gooruUId, m);
				generateAggregator(key, eventName+SEPERATOR+LASTACCESSEDUSER, userName, m);
			}
			try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
	        }
		}
		
    }
    
    public void generateCounter(String key,String columnName, long count ,MutationBatch m) {
        m.withRow(liveDashboard, key)
        .incrementCounterColumn(columnName, count);
    }
    
    public void generateAggregator(String key,String columnName, String value ,MutationBatch m) {
        m.withRow(microAggregator, key)
        .putColumnIfNotNull(columnName, value);
    }
    
	public void addRowColumn(String rowKey,String columnName,String value){

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		
		m.withRow(microAggregator, rowKey)
		.putColumnIfNotNull(columnName, value)
		;
		 try{
	         	m.execute();
	         } catch (ConnectionException e) {
	         	logger.info("Error while adding session - ", e);
	         }
	}
	
	private boolean isRowAvailable(String key,String  columnName){
		ColumnList<String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(microAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		if (result != null && !result.isEmpty() && result.getColumnByName(columnName) != null) {
				return true;
    	}		
		return false;
		
	}
	
	public List<String> generateYMWDKey(String eventTime){
		List<String> returnDate = new ArrayList<String>();	
		if(dashboardKeys != null){
			for(String key : dashboardKeys.split(",")){
				customDateFormatter = new SimpleDateFormat(key);
				Date eventDateTime = new Date(Long.valueOf(eventTime));
				String rowKey = customDateFormatter.format(eventDateTime).toString();
		        returnDate.add(rowKey);
		        
			}
		}
		return returnDate; 
	}
}
