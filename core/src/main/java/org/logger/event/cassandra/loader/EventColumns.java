package org.logger.event.cassandra.loader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventColumns implements Constants{

	public static final Map<String, Object> COLLECTION_PLAY_COLUMNS;
	public static final Map<String, Object> COLLECTION_RESOURCE_PLAY_COLUMNS;
	public static final Map<String, Object> CREATE_REACTION_COLUMNS;
	public static final Map<String, Object> USER_FEEDBACK_COLUMNS;
	public static final Map<String, Object> SCORE_AGGREGATE_COLUMNS;
	
	static { 
		Map<String, Object> cpColumns = new HashMap<String, Object>();
		cpColumns.put(VIEWS, VIEWS_COUNT);
		cpColumns.put(TIME_SPENT, TOTALTIMEINMS);
		cpColumns.put(_COLLECTION_TYPE, COLLECTION_TYPE);
		cpColumns.put(EVIDENCE, EVIDENCE);
		SCORE_AGGREGATE_COLUMNS = Collections.unmodifiableMap(cpColumns);
	}
	static {
		Map<String, Object> cpColumns = new HashMap<String, Object>();
		cpColumns.put(VIEWS, VIEWS_COUNT);
		cpColumns.put(ATTEMPTS, ATTEMPT_COUNT);
		cpColumns.put(TIME_SPENT, TOTALTIMEINMS);
		cpColumns.put(_COLLECTION_TYPE, COLLECTION_TYPE);
		cpColumns.put(EVIDENCE, EVIDENCE);
		cpColumns.put(STATUS, TYPE);
		cpColumns.put(_GOORU_UID, GOORUID);
		COLLECTION_PLAY_COLUMNS = Collections.unmodifiableMap(cpColumns);
	}
	static {
		Map<String, Object> crpColumns = new HashMap<String, Object>();
		crpColumns.put(VIEWS, VIEWS_COUNT);
		crpColumns.put(ATTEMPTS, ATTEMPT_COUNT);
		crpColumns.put(TIME_SPENT, TOTALTIMEINMS);
		crpColumns.put(SCORE, SCORE);
		crpColumns.put(CHOICE, TEXT);
		crpColumns.put(TYPE, QUESTION_TYPE);
		crpColumns.put(_ANSWER_OBECT, ANSWER_OBECT);
		crpColumns.put(STATUS, TYPE);
		COLLECTION_RESOURCE_PLAY_COLUMNS = Collections.unmodifiableMap(crpColumns);
	}
	
	static {
		/**
		 * This Map is not using in the current implementation
		 */
		Map<String, Object> cpColumns = new HashMap<String, Object>();
		CREATE_REACTION_COLUMNS = Collections.unmodifiableMap(cpColumns);
	}
	
	static {
		Map<String, Object> ufColumns = new HashMap<String, Object>();
		ufColumns.put(_FEEDBACK, TEXT);
		ufColumns.put(ACTIVE, ACTIVE);
		ufColumns.put(_FEED_BACK_TIMESTAMP, START_TIME);
		ufColumns.put(_FEED_BACK_PROVIDER, PROVIDER);
		USER_FEEDBACK_COLUMNS = Collections.unmodifiableMap(ufColumns);
	}
}
