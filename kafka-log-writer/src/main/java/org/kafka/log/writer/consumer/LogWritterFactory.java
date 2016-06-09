package org.kafka.log.writer.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogWritterFactory {

	private static final String LOG_WRITTER_NAME = "logWritterActivity";

	private static final String LOG_WRITTER_ERROR_NAME = "logWritterActivityError";

	public static final Logger activity = LoggerFactory.getLogger(LOG_WRITTER_NAME);

	public static final Logger errorActivity = LoggerFactory.getLogger(LOG_WRITTER_ERROR_NAME);

	private LogWritterFactory() {
		throw new AssertionError();
	}
}
