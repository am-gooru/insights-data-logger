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
package org.kafka.log.writer.consumer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);

	private static Properties configConstants;

    public DataLoader() {
    	loadConfigFile();
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
    	    	runThread();
			}
    	}
    	catch( ParseException exp ) {
    		LOG.error( "Unexpected exception:" + exp.getMessage() );
    	}

	}

    @Override
    public void run(){
    	runThread();
    }

    public void shutdownLogConsumer(){
    	KafkaLogConsumer.shutdownLogConsumer();
    }

	public static void runThread() {
		KafkaLogConsumer consumerThread = new KafkaLogConsumer();
        consumerThread.start();
	}

	private void loadConfigFile() {
		InputStream inputStream;
		try {
			configConstants = new Properties();
			String propFileName = "logapi-config.properties";
			String configPath = System.getenv("CATALINA_HOME").concat("/conf/");
			inputStream = FileUtils.openInputStream(new File(configPath.concat(propFileName)));
			configConstants.load(inputStream);
			inputStream.close();
		} catch (IOException ioException) {
			LOG.error("Unable to Load Config File", ioException);
		}
	}

	public static String getProperty(String key) {
		return configConstants.getProperty(key);
	}
}
