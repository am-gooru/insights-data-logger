package org.logger.event.cassandra.loader.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
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
    
    private SimpleDateFormat secondsDateFormatter;

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
        this.secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
    }

    @Async
    public void callCounters(Map<String,String> eventMap) throws JSONException, ParseException, ConnectionException {
		if(eventMap.containsKey(EVENTNAME) && eventMap.containsKey(GOORUID)) {
			String gooruUId = eventMap.get(GOORUID);
			String eventName = eventMap.get(EVENTNAME);
			String createdOn = eventMap.get(STARTTIME);
			String userName = dimUser.getUserName(gooruUId);
			
			boolean isRowAvailable =  this.isRowAvailable(METRICS, eventName);
			
			logger.info("Is row available : {} ", isRowAvailable);
			
			if(!isRowAvailable){
				this.addRowColumn(METRICS, eventName, String.valueOf(TimeUUIDUtils.getUniqueTimeUUIDinMillis()));
			}
			List<String> keys = this.generateYMWDKey();
			
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for(String key : keys) {
				generateCounter(key,eventName,1, m);
				generateCounter(key+SEPERATOR+gooruUId,eventName,1, m);
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
	
	public List<String> generateYMWDKey(){

		/*Calendar currentDate = Calendar.getInstance(); //Get the current date			
		int week = currentDate.get(Calendar.WEEK_OF_MONTH);
		int month = currentDate.get(Calendar.MONTH);
		month = month + 1;
		int year = currentDate.get(Calendar.YEAR);
		int date = currentDate.get(Calendar.DATE);
		List<String> returnDate = new ArrayList<String>();			
		
		returnDate.add(String.valueOf(year)+month+date);
		returnDate.add(String.valueOf(year)+month+week);
		returnDate.add(String.valueOf(year)+month);
		returnDate.add(String.valueOf(year));
		returnDate.add("ALL"); */

		String updatedMonth ="0";
		String updatedWeek = "0";
		String updatedDate = "0";
		Calendar currentDate = Calendar.getInstance(TimeZone.getTimeZone("UTC")); //Get the current date			
		int week = currentDate.get(Calendar.WEEK_OF_MONTH);
		int month = currentDate.get(Calendar.MONTH);
		month = month + 1;
		if(month < 10) {
			updatedMonth +=month;
		}else{
			updatedMonth =""+month;
		}
		if(week < 10) {
			updatedWeek +=week;
		}else{
			updatedWeek =""+week;
		}
		int year = currentDate.get(Calendar.YEAR);
		int date = currentDate.get(Calendar.DATE);
		if(date < 10) {
			updatedDate +=date;
		}else{
			updatedDate =""+date;
		}
		List<String> returnDate = new ArrayList<String>();			
		
		returnDate.add("D~"+year+""+updatedMonth+""+updatedDate);
		returnDate.add("W~"+year+""+updatedMonth+""+updatedWeek);
		returnDate.add("M~"+year+""+updatedMonth);
		returnDate.add("Y~"+year);
		returnDate.add("all");
		return returnDate; 
		
		
	}
}
