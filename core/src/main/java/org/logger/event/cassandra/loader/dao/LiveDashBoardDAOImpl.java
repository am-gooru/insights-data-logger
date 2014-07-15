package org.logger.event.cassandra.loader.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.ednovo.data.model.GeoData;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
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
    
    private SimpleDateFormat hourlyDateFormatter = new SimpleDateFormat("yyyyMMddkk");

    String dashboardKeys = null;
    
    ColumnList<String> eventKeys = null;
    
	String visitor = "visitor";
	
	String visitorType = "loggedInUser";
	
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
    public void callCounters(Map<String,String> eventMap) {
		if(eventMap.containsKey(EVENTNAME) && eventMap.containsKey(GOORUID)) {
			String gooruUId = eventMap.get(GOORUID);
			String eventName = eventMap.get(EVENTNAME);
			String createdOn = eventMap.get(STARTTIME);
			//String userName = dimUser.getUserName(gooruUId);
			String organizationUId = DEFAULT_ORGANIZATION_UID;
			if(eventMap.get(ORGANIZATIONUID) != null && !eventMap.get(ORGANIZATIONUID).isEmpty()) {
					organizationUId = eventMap.get(ORGANIZATIONUID);
			}
			
			/*boolean isRowAvailable =  this.isRowAvailable(METRICS, eventName);
			
			logger.info("Is row available : {} ", isRowAvailable);
			
			if(!isRowAvailable){*/
				this.addRowColumn(METRICS, eventName, String.valueOf(TimeUUIDUtils.getUniqueTimeUUIDinMillis()));
			//}
			List<String> keys = this.generateYMWDKey(eventMap.get(STARTTIME));
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			
			for(String key : keys) {
				if(eventMap.containsKey(TYPE) && eventMap.get(TYPE).equals(START)) {
					// Increment the view using start events
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					if(eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null && !eventMap.get(CONTENTGOORUOID).isEmpty()) {
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);						
					}
				}					
				
				if(!eventMap.containsKey(TYPE)) {
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
				}
				
				if(eventMap.containsKey(ITEMTYPE)){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
							generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
							generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
							generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
							generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
							generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
							generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
							generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
							generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						}
				}
				
				if(eventMap.containsKey(ACTIONTYPE)){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ACTIONTYPE)+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					}
				}
				
				if(eventMap.containsKey(USERAGENT) && eventMap.get(USERAGENT) != null && !eventMap.get(USERAGENT).isEmpty()) {
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(USERAGENT).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(USERAGENT).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					if(eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null && !eventMap.get(CONTENTGOORUOID).isEmpty()) {
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(USERAGENT).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(USERAGENT).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(USERAGENT).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(USERAGENT).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					}
				}
				
				if(eventMap.containsKey(REGISTERTYPE) && eventMap.get(REGISTERTYPE) != null && !eventMap.get(REGISTERTYPE).isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase()+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase()+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase()+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(REGISTERTYPE).toLowerCase()+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
				}
				
				if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					if(eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null && !eventMap.get(CONTENTGOORUOID).isEmpty()) {
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					}
				}
				
				if(eventMap.containsKey(MODE) && eventMap.get(MODE) != null && !eventMap.get(MODE).isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					if(eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null && !eventMap.get(CONTENTGOORUOID).isEmpty()) {
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					}
					if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE),1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						if(eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null && !eventMap.get(CONTENTGOORUOID).isEmpty()) {
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
							generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						}
					}
				}
				
				if(eventMap.containsKey(ITEMTYPE) && eventMap.containsKey(MODE) && eventMap.get(MODE) != null && !eventMap.get(MODE).isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					
					if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
						generateCounter(key+SEPERATOR+eventMap.get(ITEMTYPE)+SEPERATOR+eventMap.get(MODE)+SEPERATOR+organizationUId+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					}
				}
				
				if(eventMap.containsKey("country") && eventMap.get("country") != null && !eventMap.get("country").isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("country"),1, m);
					generateCounter(key+SEPERATOR+eventMap.get("country"),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("country"),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get("country"),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("country")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get("country")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("country")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get("country")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					}
				}
				
				if(eventMap.containsKey("state") && eventMap.get("state") != null && !eventMap.get("state").isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("state"),1, m);
					generateCounter(key+SEPERATOR+eventMap.get("state"),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("state"),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get("state"),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("state")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get("state")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("state")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get("state")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					}
				}
				
				if(eventMap.containsKey("city") && eventMap.get("city") != null && !eventMap.get("city").isEmpty()){
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("city"),1, m);
					generateCounter(key+SEPERATOR+eventMap.get("city"),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("city"),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get("city"),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					if(eventMap.containsKey(CLIENTSOURCE) && eventMap.get(CLIENTSOURCE) != null && !eventMap.get(CLIENTSOURCE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("city")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+eventMap.get("city")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get("city")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+eventMap.get("city")+SEPERATOR+eventMap.get(CLIENTSOURCE).toLowerCase(),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					}
				}
				
				if(eventMap.containsKey(TEXT) && eventMap.get(TEXT) != null && !eventMap.get(TEXT).isEmpty()) {
					generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT),1, m);
					generateCounter(key+SEPERATOR+eventMap.get(TEXT),LoaderConstants.COUNT.getName()+SEPERATOR+eventName,1, m);
					generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT),1, m);
					generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT),1, m);
					generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT),1, m);
					if(eventMap.containsKey(ITEMTYPE) && eventMap.get(ITEMTYPE) != null && !eventMap.get(ITEMTYPE).isEmpty()){
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						
						generateCounter(key,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT)+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT)+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT)+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
						generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.COUNT.getName()+SEPERATOR+eventName+SEPERATOR+eventMap.get(TEXT)+SEPERATOR+eventMap.get(ITEMTYPE),1, m);
					}
				}
				
				generateCounter(key,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
				generateCounter(key+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
				generateCounter(key+SEPERATOR+organizationUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
				generateCounter(key+SEPERATOR+organizationUId+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);						
				
				if(eventMap.containsKey(CONTENTGOORUOID) && eventMap.get(CONTENTGOORUOID) != null && !eventMap.get(CONTENTGOORUOID).isEmpty()) {
					generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID),LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
					generateCounter(key+SEPERATOR+eventMap.get(CONTENTGOORUOID)+SEPERATOR+gooruUId,LoaderConstants.TS.getName()+SEPERATOR+eventName,eventMap.containsKey(TOTALTIMEINMS) ? Long.valueOf(eventMap.get(TOTALTIMEINMS)) : 0L, m);
				}
				
				generateAggregator(key, eventName+SEPERATOR+LASTACCESSED, createdOn, m);
				generateAggregator(key, eventName+SEPERATOR+LASTACCESSEDUSERUID, gooruUId, m);
				//generateAggregator(key, eventName+SEPERATOR+LASTACCESSEDUSER, userName, m);
			}
			try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
	        }
		}
		
		try {
			this.findDifferenceInCount(eventMap);
		} catch (ParseException e) {
			logger.info("Exception while finding difference : {} ",e);
		}
    }
    
    public void callCountersV2(Map<String,String> eventMap) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	if((eventMap.containsKey(EVENTNAME) && (eventMap.get("eventName").equalsIgnoreCase("collection.play")))) {
            eventKeys = configSettings.getColumnList(eventMap.get("eventName")+SEPERATOR+"columnkey");
            for(int i=0 ; i < eventKeys.size() ; i++ ){
            	String columnName = eventKeys.getColumnByIndex(i).getName();
            	String columnValue = eventKeys.getColumnByIndex(i).getStringValue();
        		String key = this.generateKeys(eventMap.get(STARTTIME),columnName);
        		String setData = null ;
            	for(String value : columnValue.split(",")){
            		for(String getData : value.split("~")){
            			if(getData.equalsIgnoreCase("count")) {
            				setData = LoaderConstants.COUNT.getName();
            			} else if(getData.equalsIgnoreCase("time_spent")) {
            				setData = LoaderConstants.TS.getName();
            			} else { 
            				setData += SEPERATOR+eventMap.get(getData);
            			}
            		}
            		this.generateCounter(key, setData, 1, m);
            	}
            }
    	}
    }
    
    @Async
    public void pushEventForAtmosphere(String atmosphereEndPoint, Map<String,String> eventMap) throws JSONException{
   		
    	JSONObject filtersObj = new JSONObject();
    	filtersObj.put("eventName", eventMap.get("eventName"));

		JSONObject mainObj = new JSONObject();
		mainObj.put("filters", filtersObj);		

    	ClientResource clientResource = null;
    	logger.info("atmosphereEndPoint : {} " ,atmosphereEndPoint);
    	clientResource = new ClientResource(atmosphereEndPoint+"/atmosphere/push/message");
    	Form forms = new Form();
		forms.add("data", mainObj.toString());
		clientResource.post(forms.getWebRepresentation());
       
    }
    
    @Async
    public void findDifferenceInCount(Map<String,String> eventMap) throws ParseException{
    	
    	Map<String,String>  aggregator = this.generateKeyValues(eventMap);
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    	
    	for (Map.Entry<String, String> entry : aggregator.entrySet()) {
    		
    	    long thisCount = this.getLiveLongValue(entry.getKey(), COUNT+SEPERATOR+eventMap.get(EVENTNAME));
    	    long lastCount = this.getLiveLongValue(entry.getValue(), COUNT+SEPERATOR+eventMap.get(EVENTNAME));
    	    if(lastCount != 0L){
    	    	long difference = (thisCount*100)/lastCount;
    	    	this.generateAggregator(entry.getKey()+SEPERATOR+entry.getValue(), DIFF+SEPERATOR+eventMap.get(EVENTNAME), String.valueOf(difference), m);
    	    }
    	}    	
    	try {
            m.execute();
        } catch (ConnectionException e) {
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
			this.addRowColumn(rowKey, columnName, json);
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
    
	private Long getLiveLongValue(String key,String  columnName){
		
		ColumnList<String>  result = null;
		long value = 0L;
    	try {
    		 result = getKeyspace().prepareQuery(liveDashboard)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		if(result != null && result.getColumnByName(columnName) != null){
			value = result.getLongValue(columnName,0L);
		}
		return value;
	}
	
	private String getMicroStringValue(String key,String  columnName){
		ColumnList<String>  result = null;
		String value = null;
    	try {
    		 result = getKeyspace().prepareQuery(microAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		if(result != null && result.getColumnByName(columnName) != null){
			value = result.getStringValue(columnName, null);
		}
		return value;
	}
	
	private ColumnList<String> getMicroColumnList(String key){
		ColumnList<String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(microAggregator)
    		 .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
        		    .getKey(key)
        		    .execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while retieveing data from readViewCount: {}" ,e);
		}
		return result;
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

	public void addApplicationSession(Map<String,String> eventMap){
		
		String dateKey = hourlyDateFormatter.format(new Date()).toString();
		
		logger.info("dateKey : {}",dateKey);
		
		if(eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")){
			visitorType = "anonymousUser";
		}
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		if(!this.isRowAvailable(dateKey, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID))){
			this.generateCounter(visitor, visitorType, 1, m);
		}
	
		logger.info("values : {}", customDateFormatter.format(new Date()).toString());
		this.generateAggregator(dateKey, eventMap.get(SESSIONTOKEN)+SEPERATOR+eventMap.get(GOORUID), customDateFormatter.format(new Date()).toString(), m);
		
		try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}
	
	public void watchApplicationSession() throws ParseException{
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		String lastUpdated = configSettings.getConstants("last~updated~session","constant_value");
		String currentHour = hourlyDateFormatter.format(new Date()).toString();

		if(lastUpdated == null || lastUpdated.equals(currentHour)){
			this.updateExpiredToken(currentHour);
		}else{
			ColumnList<String> tokenList= this.getMicroColumnList(lastUpdated);
		 	for(int i = 0 ; i < tokenList.size() ; i++) {
		 		String column = tokenList.getColumnByIndex(i).getName();
		 		String value = tokenList.getColumnByIndex(i).getStringValue();
		 		String[] parts = column.split("~");
		 		
		 		if(parts[1].equalsIgnoreCase("ANONYMOUS")){
					visitorType = "anonymousUser";
				}
		 		if(!value.equalsIgnoreCase("expired") && this.isRowAvailable(currentHour, column)){
		 			this.generateCounter(visitor, visitorType, -1, m);
		 		}else{
		 			this.generateAggregator(currentHour, column,value, m);
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
		ColumnList<String> tokenList= this.getMicroColumnList(timeLine);
	 	for(int i = 0 ; i < tokenList.size() ; i++) {
	 		String column = tokenList.getColumnByIndex(i).getName();
	 		String value = tokenList.getColumnByIndex(i).getStringValue();
	 		String[] parts = column.split("~");
	 		
	 		if(parts[1].equalsIgnoreCase("ANONYMOUS")){
				visitorType = "anonymousUser";
			}
	 		if(!value.equalsIgnoreCase("expired")){
	 			Date valueInDate = customDateFormatter.parse(value);
	 			int diffInMinutes = (int)( (new Date().getTime() - valueInDate.getTime() ) / ((60 * 1000) % 60)) ;
	 			if(diffInMinutes > 30){
	 				this.generateAggregator(visitor, visitorType, "expired", m);
	 				this.generateCounter(visitor, visitorType, -1, m);
	 			}
	 		}
	 	}
	 	try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("updateCounter => Error while inserting to cassandra {} ", e);
        }
	}
	
	public List<String> generateYMWDKey(String eventTime){
		List<String> returnDate = new ArrayList<String>();	
		if(dashboardKeys != null){
			for(String key : dashboardKeys.split(",")){
				String rowKey = null;
				if(!key.equalsIgnoreCase("all")) {
					customDateFormatter = new SimpleDateFormat(key);
					Date eventDateTime = new Date(Long.valueOf(eventTime));
				try{					
					rowKey = customDateFormatter.format(eventDateTime).toString();
				}
				catch(Exception e){
					logger.info("Exception while key generation : {} ",e);
				}
				} else {
					rowKey = key;
				}
		        returnDate.add(rowKey);
			}
		}
		return returnDate; 
	}
	
	public String generateKeys(String eventTime,String columnName){
		String rowKey = null;
		if(eventTime != null){
				String[] key = columnName.split("~");
				String newKey = key[0];
				if(!newKey.equalsIgnoreCase("all")) {
					customDateFormatter = new SimpleDateFormat(newKey);
					Date eventDateTime = new Date(Long.valueOf(eventTime));
				try{					
					rowKey = customDateFormatter.format(eventDateTime).toString();
				}
				catch(Exception e){
					logger.info("Exception while key generation : {} ",e);
				}
				} else {
					rowKey = newKey;
				}
		}
		return rowKey; 
	}
}
