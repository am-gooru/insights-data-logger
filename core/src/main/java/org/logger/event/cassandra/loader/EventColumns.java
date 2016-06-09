package org.logger.event.cassandra.loader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class EventColumns {

	private static final Map<String, Object> COLLECTION_PLAY_COLUMNS;
	private static final Map<String, Object> COLLECTION_RESOURCE_PLAY_COLUMNS;
	private static final Map<String, Object> CREATE_REACTION_COLUMNS;
	private static final Map<String, Object> USER_FEEDBACK_COLUMNS;
	private static final Map<String, Object> SCORE_AGGREGATE_COLUMNS;

	static {
		Map<String, Object> cpColumns = new HashMap<>();
		cpColumns.put(Constants.VIEWS, Constants.VIEWS_COUNT);
		cpColumns.put(Constants.TIME_SPENT, Constants.TOTALTIMEINMS);
		cpColumns.put(Constants._COLLECTION_TYPE, Constants.COLLECTION_TYPE);
		cpColumns.put(Constants.EVIDENCE, Constants.EVIDENCE);
		SCORE_AGGREGATE_COLUMNS = Collections.unmodifiableMap(cpColumns);
	}
	static {
		Map<String, Object> cpColumns = new HashMap<>();
		cpColumns.put(Constants.VIEWS, Constants.VIEWS_COUNT);
		cpColumns.put(Constants.ATTEMPTS, Constants.ATTEMPT_COUNT);
		cpColumns.put(Constants._COLLECTION_TYPE, Constants.COLLECTION_TYPE);
		cpColumns.put(Constants.EVIDENCE, Constants.EVIDENCE);
		cpColumns.put(Constants.STATUS, Constants.TYPE);
		COLLECTION_PLAY_COLUMNS = Collections.unmodifiableMap(cpColumns);
	}
	static {
		Map<String, Object> crpColumns = new HashMap<>();
		crpColumns.put(Constants.VIEWS, Constants.VIEWS_COUNT);
		crpColumns.put(Constants.ATTEMPTS, Constants.ATTEMPT_COUNT);
		crpColumns.put(Constants.TIME_SPENT, Constants.TOTALTIMEINMS);
		crpColumns.put(Constants.TYPE, Constants.QUESTION_TYPE);
		crpColumns.put(Constants.STATUS, Constants.TYPE);
		COLLECTION_RESOURCE_PLAY_COLUMNS = Collections.unmodifiableMap(crpColumns);
	}

	static {
		/**
		 * This Map is not using in the current implementation
		 */
		Map<String, Object> cpColumns = new HashMap<>();
		CREATE_REACTION_COLUMNS = Collections.unmodifiableMap(cpColumns);
	}

	static {
		Map<String, Object> ufColumns = new HashMap<>();
		ufColumns.put(Constants._FEEDBACK, Constants.TEXT);
		ufColumns.put(Constants.ACTIVE, Constants.ACTIVE);
		ufColumns.put(Constants._FEED_BACK_TIMESTAMP, Constants.START_TIME);
		ufColumns.put(Constants._FEED_BACK_PROVIDER, Constants.PROVIDER);
		USER_FEEDBACK_COLUMNS = Collections.unmodifiableMap(ufColumns);
	}

	private EventColumns() {
		throw new AssertionError();
	}
}
