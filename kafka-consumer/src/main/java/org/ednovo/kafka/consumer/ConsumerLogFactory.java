package org.ednovo.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsumerLogFactory {

	private static final String CONSUMER_LOGGER_NAME = "consumerActivity";

	private static final String CONSUMER_ERROR_LOGGER_NAME = "consumerActivityError";

	public static final Logger activity = LoggerFactory.getLogger(CONSUMER_LOGGER_NAME);

	public static final Logger errorActivity = LoggerFactory.getLogger(CONSUMER_ERROR_LOGGER_NAME);

	private ConsumerLogFactory() {
		throw new AssertionError();
	}
}
