package org.kafka.event.microaggregator.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregatorLogFactory {

	private static final String AGGREGATOR_NAME = "aggregatorActivity";
	
	private static final String AGGREGATOR_ERROR_NAME = "aggregatorActivityError";
	
	public static final Logger activity = LoggerFactory.getLogger(AGGREGATOR_NAME);
	
	public static final Logger errorActivity = LoggerFactory.getLogger(AGGREGATOR_ERROR_NAME);
}
