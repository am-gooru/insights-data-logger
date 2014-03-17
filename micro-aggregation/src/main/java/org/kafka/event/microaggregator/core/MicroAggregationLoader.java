/*******************************************************************************
 * CassandraDataLoader.java
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
package org.kafka.event.microaggregator.core;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.UUID;

import org.json.JSONException;
import org.kafka.event.microaggregator.dao.CounterDetailsDAO;
import org.kafka.event.microaggregator.dao.CounterDetailsDAOCassandraImpl;
import org.kafka.event.microaggregator.dao.JobConfigSettingsDAOCassandraImpl;
import org.kafka.event.microaggregator.dao.MicroAggregationDAOImpl;
import org.kafka.event.microaggregator.dao.RealTimeOperationConfigDAOImpl;
import org.kafka.event.microaggregator.dao.RecentViewedResourcesDAOImpl;
import org.kafka.event.microaggregator.model.EventObject;
import org.kafka.event.microaggregator.model.JSONDeserializer;
import org.kafka.event.microaggregator.producer.MicroAggregatorProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;


public class MicroAggregationLoader {

    private static final Logger logger = LoggerFactory
            .getLogger(MicroAggregationLoader.class);
    private Keyspace cassandraKeyspace;
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
    private SimpleDateFormat minuteDateFormatter;
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    private CassandraConnectionProvider connectionProvider;
    
    private JobConfigSettingsDAOCassandraImpl configSettings;
    
    private RecentViewedResourcesDAOImpl recentViewedResources;
    
    private CounterDetailsDAOCassandraImpl counterDetailsDao;
    
    private CounterDetailsDAO counterDetails;
    
    private MicroAggregationDAOImpl microAggregation;
    
    private RealTimeOperationConfigDAOImpl realTimeOperation;
    
    public static  Map<String,String> realTimeOperators;
    
    private MicroAggregatorProducer microAggregator;
    
    private Gson gson = new Gson();
    /**
     * Get Kafka properties from Environment
     */
    public MicroAggregationLoader() {
        this(null);
        
        String KAFKA_IP = System.getenv("INSIGHTS_KAFKA_IP");
        String KAFKA_PORT = System.getenv("INSIGHTS_KAFKA_PORT");
        String KAFKA_ZK_PORT = System.getenv("INSIGHTS_KAFKA_ZK_PORT");
        String KAFKA_TOPIC = System.getenv("INSIGHTS_KAFKA_TOPIC");
        String KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
        String KAFKA_AGGREGATOR_TOPIC = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_TOPIC");
        String KAFKA_PRODUCER_TYPE = System.getenv("INSIGHTS_KAFKA_PRODUCER_TYPE");
        
        microAggregator = new MicroAggregatorProducer(KAFKA_IP, KAFKA_ZK_PORT,  KAFKA_AGGREGATOR_TOPIC, KAFKA_PRODUCER_TYPE);
    }

    public MicroAggregationLoader(Map<String, String> configOptionsMap) {
        init(configOptionsMap);
    }

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    /**
     * *
     * @param configOptionsMap
     * Initialize Coulumn Family
     */
    
    private void init(Map<String, String> configOptionsMap) {
    	
        this.minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

        this.setConnectionProvider(new CassandraConnectionProvider());
        this.getConnectionProvider().init(configOptionsMap);

        this.configSettings = new JobConfigSettingsDAOCassandraImpl(getConnectionProvider());    
        this.counterDetailsDao = new CounterDetailsDAOCassandraImpl(getConnectionProvider());
        this.recentViewedResources = new RecentViewedResourcesDAOImpl(getConnectionProvider());
        this.microAggregation = new MicroAggregationDAOImpl(getConnectionProvider());
        this.realTimeOperation = new RealTimeOperationConfigDAOImpl(getConnectionProvider());
        realTimeOperators = realTimeOperation.getOperators();

    }
    
    public void microRealTimeAggregation(String eventJSON) throws JSONException{

    	JsonObject eventObj = new JsonParser().parse(eventJSON).getAsJsonObject();
    	EventObject eventObject = gson.fromJson(eventObj, EventObject.class);
    	Map<String,String> eventMap = JSONDeserializer.deserializeEventObject(eventObject);
    	
    	eventObject.setParentGooruId(eventMap.get("parentGooruId") == null ? "NA" : eventMap.get("parentGooruId"));
    	eventObject.setContentGooruId(eventMap.get("contentGooruId") == null ? "NA" : eventMap.get("contentGooruId"));
    	eventObject.setTimeInMillSec(eventMap.get("totalTimeSpentInMs") == null ? 0L : Long.parseLong(eventMap.get("totalTimeSpentInMs")));
    	eventObject.setEventType(eventMap.get("type"));
    	eventMap.put("eventName", eventObject.getEventName());
    	eventMap.put("eventId", eventObject.getEventId());
    	
    	String aggregatorJson = realTimeOperators.get(eventMap.get("eventName"));
		 
		 if(aggregatorJson != null && !aggregatorJson.isEmpty()){
			 counterDetailsDao.realTimeMetrics(eventMap, aggregatorJson);
		 }
    }

    /**
     * @return the connectionProvider
     */
    public CassandraConnectionProvider getConnectionProvider() {
    	return connectionProvider;
    }
    
    /**
     * @param connectionProvider the connectionProvider to set
     */
    public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
    	this.connectionProvider = connectionProvider;
    }
    
    private ColumnFamilyQuery<String, String> prepareQuery(ColumnFamily<String, String> columnFamily) {
    	return cassandraKeyspace.prepareQuery(columnFamily).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    }
    
}

