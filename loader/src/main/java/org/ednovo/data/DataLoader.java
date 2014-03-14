/*******************************************************************************
 * DataLoader.java
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
package org.ednovo.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.ednovo.data.handlers.CassandraProcessor;
import org.ednovo.data.handlers.DataProcessor;
import org.ednovo.data.handlers.FileInputProcessor;
import org.ednovo.data.handlers.JSONProcessor;
import org.ednovo.data.handlers.PSVProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader  {
    protected static Properties properties;
    static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);

    public DataLoader() {
		
	}
    
    public static void main(String[] args) throws java.text.ParseException {
    	// create the command line parser
    	CommandLineParser parser = new PosixParser();

    	// create the Options
    	Options options = new Options();
    
    	options.addOption("psv","psv-path", true, "process csv files");
    	options.addOption( "f", "file-path", true, "process files from path" );
    	options.addOption( "p", "path-pattern", true, "File path pattern" );
    	options.addOption( "cf", "column-family", true, "the column family to use, defaults to ActivityLog" );
    	options.addOption( "s", "hosts", true, "The list of hosts to connect to. In the format IP:port. " );
    	options.addOption( "ks", "keyspace", true, "Keyspace to use" );
    	options.addOption( "k", "kafka-stream", false, "process messages from kafka stream" );
    	options.addOption( "st", "startTime", true, "StartTime to process stagind data" );
    	options.addOption( "et", "endTime", true, "EndTime to process stagind data" );
    	options.addOption( "en", "eventName", true, "Load particular event in staging" );
    	
    	options.addOption( "dryRun", "dryRun", true, "DryRun. pass true to make a dryrun. default true" );
        options.addOption( "tsStart", "tsStart", true, "time stamp Start. start of timestamp" );
        options.addOption( "tsStop", "tsStop", true, "time stamp Stop. End of timestamp" );
    	options.addOption( "cmd", "command", true, "Ad-hoc command to pass, primarily for ad-hoc testing" );
    	options.addOption( "geoLocationUpdate", "geoLocationUpdate", true, "geoLocationUpdate" );
    	options.addOption( "updateViewCount", "updateViewCount", true, "updateViewCount" );
    	options.addOption( "runAggregation", "runAggregation", true, "runAggregation" );
    	options.addOption( "gooruOid", "gooruOid", true, "gooruOid" );
    	options.addOption( "viewCount", "viewCount", true, "viewCount" );
    	options.addOption( "aggregators", "aggregators", true, "aggregators" );
    	options.addOption( "updateBy", "updateBy", true, "updateBy" );
    	options.addOption( "callAPIViewCount", "callAPIViewCount", true, "callAPIViewCount" );

    	try {
    	    // parse the command line arguments
    	    CommandLine line = parser.parse( options, args );
    	    Map<String, String> configOptionsMap = new HashMap<String, String>();
    	    populateOptionValue(line, configOptionsMap, "column-family", "column-family");
    	    populateOptionValue(line, configOptionsMap, "hosts", "hosts");
    	    populateOptionValue(line, configOptionsMap, "keyspace", "keyspace");

    	    if(line.hasOption("command")) {
    	    	String cmd  = line.getOptionValue( "command");
    	    	if(cmd.equalsIgnoreCase("delete-events") || cmd.equalsIgnoreCase("delete-staging")) {
	    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);	
	    	    	String timeStampStartMinute = null;
                        String timeStampStopMinute = null;
                        boolean dryRun = true;
                        if(line.hasOption("tsStart")) {
                            timeStampStartMinute = line.getOptionValue( "tsStart");
                        }
                        if(line.hasOption("tsStop")) {
                            timeStampStopMinute = line.getOptionValue( "tsStop");
                        }
                        if(timeStampStartMinute == null || timeStampStartMinute.isEmpty()||timeStampStopMinute == null ||timeStampStopMinute.isEmpty()) {
                            System.err.println("Timestamp start / stop are mandatory. pass tsStart / tsStop");
                        }
                        if(line.hasOption("dryRun")) {
                            String dryRunValue = line.getOptionValue("dryRun");
                            if(dryRunValue != null && (dryRunValue.equalsIgnoreCase("0")||dryRunValue.equalsIgnoreCase("false"))) {
                               dryRun = false; 
                            }
                        }
                        
                        
	    	    	return;
    	    	}
    	    }
    	    // validate that block-size has been set
    	    if( line.hasOption( "file-path" ) ) {
    	    	LOG.info("processing files");
        	    populateOptionValue(line, configOptionsMap, "path-pattern", "path-pattern", "activity*.log");
        	    populateOptionValue(line, configOptionsMap, "file-path", "file-path");
    	    	
    	    	DataProcessor[] handlers = {new FileInputProcessor(configOptionsMap), new JSONProcessor(), new CassandraProcessor(configOptionsMap)};
				DataProcessor initialRowHandler = buildHandlerChain(handlers);
				initialRowHandler.processRow(null);
				
    	        // print the value of block-size
    		    return;
    	    }
    	    //for csv files to get processed
    	    if( line.hasOption( "psv-path" ) ) {
    	    	LOG.info("processing files");
        	    populateOptionValue(line, configOptionsMap, "path-pattern", "path-pattern");
        	    populateOptionValue(line, configOptionsMap, "psv-path", "file-path", "*.psv");

    	    	DataProcessor[] handlers = {new FileInputProcessor(configOptionsMap), new PSVProcessor(), new CassandraProcessor(configOptionsMap)};
				DataProcessor initialRowHandler = buildHandlerChain(handlers);
				initialRowHandler.processRow(null);
    		    return;
    	    }
    	    
    	    //for update resource view count
    	    if( line.hasOption( "updateViewCount" ) ) {
    	    	LOG.info("Updating view count");
    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);
    	    	long viewcount;
    	    	viewcount = Long.parseLong(line.getOptionValue("viewCount"));
    	    	cassandraProcessor.updateViewCount(line.getOptionValue("gooruOid"), viewcount);
    	    }
    	    
    	    if( line.hasOption( "aggregators" ) ) {
    	    	LOG.info("Updating aggregators");
    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);
    	    	cassandraProcessor.addAggregators(line.getOptionValue("eventName"), line.getOptionValue("aggregators"),line.getOptionValue("updateBy"));
    	    }
    	    
    	    //for pig aggregation
    	    if(line.hasOption("runAggregation")) {
    	    	LOG.info("Aggregation Starts");
    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);
    	    	String eventName;
    	    	eventName = line.getOptionValue("eventName");
    	    	cassandraProcessor.runPig(eventName);
    	    }
    	    
    	    //call gooru-appi update resource view count
    	    if( line.hasOption( "callAPIViewCount" ) ) {
    	    	LOG.info("call API view count");
    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);
    	    	cassandraProcessor.callAPIViewCount();
    	    }
    	    
    	    if (line.hasOption("geoLocationUpdate") && line.hasOption("startTime") && line.hasOption("endTime")) {
    	    	LOG.info("processing staging data for geo-location-update");
    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);
    	    	cassandraProcessor.geoLocationUpdate(line.getOptionValue("startTime"), line.getOptionValue("endTime"));
    	    } else {
	    	    if (line.hasOption("startTime") && line.hasOption("endTime")) {
	    	    	String EventName = null;
	    	    	if(line.getOptionValue("eventName") != null || line.getOptionValue("eventName") == ""){
	    	    		EventName = line.getOptionValue("eventName");
	    	    		//EventName = EventName.replaceAll("'", "");
	    	    		if(EventName.equalsIgnoreCase("all")){
	    	    			EventName = null;	
	    	    		}
	    	    	}
	    	    	LOG.info("processing staging data");
	    	    	LOG.info("eventName : {} ",line.getOptionValue("eventName"));
	    	    	CassandraProcessor cassandraProcessor = new CassandraProcessor(configOptionsMap);
	    	    	cassandraProcessor.updateToStaging(line.getOptionValue("startTime"), line.getOptionValue("endTime"),EventName);
	    	    }
    	    }
    	}
    	catch( ParseException exp ) {
    	    System.out.println( "Unexpected exception:" + exp.getMessage() );
    	}
	}
    
    private static void populateOptionValue(CommandLine line, Map<String, String> configOptionsMap, String optionName, String mapKeyName) {
    	populateOptionValue(line, configOptionsMap, optionName, mapKeyName, null);
    }
    
    private static void populateOptionValue(CommandLine line, Map<String, String> configOptionsMap, String optionName, String mapKeyName, String defaultValue) {
    	String optionValue = null;
    	
    	if( line.hasOption( optionName ) ) {
    		optionValue = line.getOptionValue( optionName );
	    }
    	
    	if(optionValue == null && defaultValue != null) {
    		optionValue = defaultValue;
    	}
    		
    	if(optionValue != null) {
    		configOptionsMap.put(mapKeyName, optionValue);
    	}
    }
    
    public static DataProcessor buildHandlerChain(DataProcessor[] handlers) {
    	DataProcessor firstHandler = null;
    	DataProcessor currentHandler = null;
    	for (int handlerIndex = 0; handlerIndex < handlers.length; handlerIndex++) {
			if(handlerIndex == 0) {
				firstHandler = handlers[handlerIndex];
			}
			currentHandler = handlers[handlerIndex];
			
			if(handlers.length > (handlerIndex + 1)) {
				currentHandler.setNextRowHandler(handlers[handlerIndex + 1]);
			}
		}
    	return firstHandler;
    }
    
}
