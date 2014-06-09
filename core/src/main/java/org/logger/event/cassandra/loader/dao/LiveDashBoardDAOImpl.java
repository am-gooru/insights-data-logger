package org.logger.event.cassandra.loader.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class LiveDashBoardDAOImpl  extends BaseDAOCassandraImpl implements LiveDashBoardDAO,Constants{

	private static final Logger logger = LoggerFactory.getLogger(CounterDetailsDAOCassandraImpl.class);

    private final ColumnFamily<String, String> microAggregator;
    
    private static final String CF_MICRO_AGGREGATOR = "micro_aggregation";

    private final ColumnFamily<String, String> liveDashboard;
    
    private static final String CF_LIVE_DASHBOARD = "live_dashboard";

    private CassandraConnectionProvider connectionProvider;
    
    private CollectionItemDAOImpl collectionItem;
    
    private EventDetailDAOCassandraImpl eventDetailDao; 
    
    private DimResourceDAOImpl dimResource;
    
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
        this.secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
    }

    @Async
    public void callCounters(Map<String,String> eventMap) throws JSONException, ParseException {
    	String contentGooruOId = "";
		String eventName = "";
		String gooruUId = "";
		long start = System.currentTimeMillis();
		if(eventMap.containsKey(EVENTNAME) && eventMap.containsKey(GOORUID)) {
			contentGooruOId = eventMap.get(CONTENTGOORUOID);
			gooruUId = eventMap.get(GOORUID);
			eventName = eventMap.get(EVENTNAME);
			String createdOn = eventMap.get(STARTTIME);
			
			boolean isRowAvailable =  this.isRowAvailable(METRICS, eventName);
			
			if(!isRowAvailable){
				this.addRowColumn(METRICS, eventName, String.valueOf(TimeUUIDUtils.getUniqueTimeUUIDinMillis()));
			}
			List<String> keys = this.generateYMWDKey();
			
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for(String key : keys) {
				generateCounter(key,eventName,1, m);
				generateCounter(key+SEPERATOR+gooruUId,eventName,1, m);
				generateAggregator(key, LASTACCESSED, createdOn, m);
				generateAggregator(key, LASTACCESSEDUSER, gooruUId, m);
			}
			try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
	        }
		}
		
		long stop = System.currentTimeMillis();
		logger.info("Time spent for counters : {}",(stop-start));	
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

		Calendar currentDate = Calendar.getInstance(); //Get the current date			
		int week = currentDate.get(Calendar.WEEK_OF_MONTH);
		int month = currentDate.get(Calendar.MONTH);
		month = month + 1;
		int year = currentDate.get(Calendar.YEAR);
		int date = currentDate.get(Calendar.DATE);
		List<String> returnDate = new ArrayList<String>();			
		
		returnDate.add(year+SEPERATOR+month+SEPERATOR+date);
		returnDate.add(year+SEPERATOR+month+SEPERATOR+week);
		returnDate.add(year+SEPERATOR+month+SEPERATOR);
		returnDate.add(year+SEPERATOR);

		return returnDate;
		
	}
}