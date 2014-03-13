/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 *  http://www.goorulearning.org/
 *  
 *  DataLoader.java
 *  event-api-stable-1.1
 *  
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *   "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 * 
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.kafka.event.microaggregator.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader  {

    static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);
	private static String KAFKA_FILE_TOPIC;

    public DataLoader() {
    	KAFKA_FILE_TOPIC = System.getenv("INSIGHTS_KAFKA_FILE_TOPIC");
	}
    
    public static void main(String[] args) throws java.text.ParseException {
    	// create the command line parser
    	CommandLineParser parser = new PosixParser();

    	// create the Options
    	Options options = new Options();
    
    	options.addOption( "k", "kafka-stream", false, "process messages from kafka stream" );

    	try {
    	    // parse the command line arguments
    	    CommandLine line = parser.parse( options, args );    	    
    	    
    	    if( line.hasOption( "kafka-stream" ) ) {
    	    	LOG.info("processing kafka stream as consumer");
    	    	MicroAggregatorConsumer consumerThread = new MicroAggregatorConsumer(KAFKA_FILE_TOPIC);
    	        consumerThread.start();
    		    return;
    	    }
    	}
    	catch( ParseException exp ) {
    	    System.out.println( "Unexpected exception:" + exp.getMessage() );
    	}
    			
	}
}
