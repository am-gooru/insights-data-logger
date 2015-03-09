package org.kafka.event.microaggregator.core;

public interface Constants {

	public static final String FIRSTSESSION = "FS~";

	public static final String RECENTSESSION = "RS~";

	public static final String ALLSESSION = "AS~";

	public static final String SEPERATOR = "~";

	public static final String CONTEXT = "context";

	public static final String USER = "user";

	public static final String PAYLOAD = "payLoadObject";

	public static final String METRICS = "metrics";

	public static final String LASTACCESSEDUSERUID = "last~accessed~useruid";

	public static final String LASTACCESSEDUSER = "last~accessed~user";

	public static final String LASTACCESSED = "last~accessed";

	public static final String EVENTNAME = "eventName";

	public static final String CONTENTGOORUOID = "contentGooruId";

	public static final String PARENTGOORUOID = "parentGooruId";

	public static final String TIMEINMS = "timeInMillSec";

	public static final String TOTALTIMEINMS = "totalTimeSpentInMs";

	public static final String EVENTIS = "eventId";

	public static final String PARENTEVENTID = "parentEventId";

	public static final String PARENT_EVENT_ID = "parent_event_id";

	// public static final String CLASSPAGEGOORUOID= "classPageGooruId";

	public static final String CLASSPAGEGOORUOID = "classpageGooruId";

	public static final String VERSION = "version";

	public static final String EVENTTYPE = "eventType";

	public static final String SESSION = "sessionId";

	public static final String GOORUID = "gooruUId";

	public static final String AGG = "aggregator";

	public static final String AGGTYPE = "aggregatorType";

	public static final String AGGMODE = "aggregatorMode";

	public static final String SUM = "sum";

	public static final String AVG = "avg";

	public static final String SUMOFAVG = "sumofavg";

	public static final String DIVIDEND = "dividend";

	public static final String DIVISOR = "divisor";

	public static final String CHOICE = "choice";

	public static final String STATUS = "question_status";

	public static final String ANSWER_OBECT = "answer_object";

	public static final String ANSWEROBECT = "answerObject";

	public static final String ATTEMPTS = "attempts";

	public static final String OPTIONS = "options";

	public static final String COUNTER = "counter";

	public static final String RESOURCETYPE = "resourceType";

	public static final String QUESTIONTYPE = "questionType";

	public static final String OE = "OE";

	public static final String TYPE = "type";

	public static final String ITEMTYPE = "itemType";

	public static final String REGISTERTYPE = "registerType";

	public static final String CLIENTSOURCE = "clientSource";

	public static final String AUTO = "auto";

	public static final String STOP = "stop";

	public static final String START = "start";

	public static final String ANS = "answers";

	public static final String TEXT = "text";

	public static final String FEEDBACK = "feed_back";

	public static final String FEEDBACKPROVIDER = "feed_back_provider";

	public static final String PROVIDER = "feedbackProviderUId";

	public static final String TIMESTAMP = "feed_back_timestamp";

	public static final String VIEWS = "views";

	public static final String QUESTION = "question";

	public static final String ATTMPTTRYSEQ = "attemptTrySequence";

	public static final String ATTMPTSTATUS = "attemptStatus";

	public static final String COLLECTION = "collection";

	public static final String RESOURCE = "resource";

	public static final String GOORUOID = "gooru_oid";

	public static final String USERID = "gooru_uid";

	public static final String CLASSPAGEID = "classpage_id";

	public static final String MINUTEDATEFORMATTER = "yyyyMMddkkmm";

	public static final String FIELDS = "fields";

	public static final String START_TIME = "start_time";

	public static final String STARTTIME = "startTime";

	public static final String END_TIME = "end_time";

	public static final String APIKEY = "apiKey";

	public static final String ORGANIZATIONUID = "organizationUId";

	public static final String EVENT_VALUE = "event_value";

	public static final String CONTENT_GOORU_OID = "content_gooru_oid";

	public static final String PARENT_GOORU_OID = "parent_gooru_oid";

	public static final String EVENT_TYPE = "event_type";

	public static final String EVENT_NAME = "event_name";

	public static final String MODE = "mode";

	public static final String STUDY = "study";

	public static final String PREVIEW = "preview";

	public static final String USER_GROUP_UID = "user_group_uid";

	public static final String CLASSPAGE_CONTENT_ID = "classpage_content_id";

	public static final String CLASSPAGE_CODE = "classpage_code";

	public static final String USER_GROUP_CODE = "user_group_code";

	public static final String CLASSPAGE_GOORU_OID = "classpage_gooru_oid";

	public static final String ORGANIZATION_UID = "organization_uid";

	public static final String RAWUPDATE = "rawupdate";

	public static final String GROUPUID = "groupUId";

	public static final String CLASSCODE = "classCode";

	public static final String CONTENTID = "contentId";

	public static final String CONTENT_ID = "content_id";

	public static final String COLLECTIONITEMID = "collectionItemId";

	public static final String COLLECTION_ITEM_ID = "collection_item_id";

	public static final String PARENTCONTENTID = "parentContentId";

	public static final String PARENT_CONTENT_ID = "parent_content_id";

	public static final String ITEMSEQUENCE = "itemSequence";

	public static final String ITEM_SEQUENCE = "item_sequence";

	public static final String COLLECTION_GOORU_OID = "collection_gooru_oid";

	public static final String RESOURCE_GOORU_OID = "resource_gooru_oid";

	public static final String SCORE = "score";

	public static final String QUESTION_COUNT = "question_count";

	public static final String SCORE_IN_PERCENTAGE = "grade_in_percentage";

	public static final String ACTIVE = "active";

	public static final String COUNT = "count";

	public static final String DIFF = "diff";

	public static final String DEFAULT_ORGANIZATION_UID = "4261739e-ccae-11e1-adfb-5404a609bd14";

	public static final String CONSTANT_VALUE = "constant_value";

	public static final String LAST_PROCESSED_TIME = "last_processed_time";

	public static final String EVENT_TIMELINE_KEY_FORMAT = "yyyyMMddkkmm";

	public static final String MINUTE_AGGREGATOR_PROCESSOR_KEY = "aggregation~events";

	public static final String FORMULA = "formula";

	public static final String COMMA = ",";

	public static final String ITEM_VALUE = "item_value";

	public static final String EMPTY = "";

	public static final String DOUBLE_QUOTES = "\"";

	public static final String AVOID_SPECIAL_CHARACTER = "[^a-zA-Z0-9]";

	public static final String CONFIG_SETTINGS = "config_settings";

	public static final String MAIL_LOOP_COUNT = "mail_loop_count";
	
	public static final String THREAD_LOOP_COUNT = "thread_loop_count";
	
	public static final String THREAD_SLEEP_TIME = "thread_sleep_time";
	
	public static final String MONITOR_KAFKA_LOG_CONSUMER = "monitor~kafka~log~consumer";
	
	public static final String MONITOR_KAFKA_AGGREGATOR_CONSUMER = "monitor~kafka~aggregator~consumer";
	
	public enum sortType {

		ASC("ASC"), DESC("DESC");

		private String sortOrder;

		private sortType(String name) {
			sortOrder = name;
		}

		public String sortType() {
			return sortOrder;
		}
	}

	public enum eventJSON {

		SESSION("session"), CONTEXT("context"), PAYLOADOBJECT("payLoadObject"), USER("user");

		private String eventJSON;

		private eventJSON(String value) {
			eventJSON = value;
		}

		public String eventJSON() {
			return eventJSON;
		}

	}

	public enum formulaDetail {

		NAME("name"), FORMULA("formula"), FORMULAS("formulas"), COMPLETED("completed"), INPROGRESS("inprogress"), ACTIVE("active"), EVENTS("events"), STATUS("status"), REQUEST_VALUES("requestValues");

		private String formulaKey;

		private formulaDetail(String value) {
			formulaKey = value;
		}

		public String formulaDetail() {
			return formulaKey;
		}
	}

	public enum columnKey {
		C("C:"), D("D:"), E("E:");

		private String column;

		private columnKey(String name) {
			column = name;
		}

		public String columnKey() {
			return column;
		}
	}

	public enum mapKey {

		KEY("key"), VALUE("value");

		private String map;

		private mapKey(String value) {
			map = value;
		}

		public String mapKey() {
			return map;
		}

	}

	public enum aggregateType {

		KEY("aggregate_type"), NORMAL("normal"), AGGREGATE("aggregate");

		private String aggregateName;

		private aggregateType(String value) {
			aggregateName = value;
		}

		public String aggregateType() {
			return aggregateName;
		}

	}

	public enum columnFamily {

		LIVE_DASHBOARD("live_dashboard"), AGGREGATION_DETAIL("aggregation_detail"), JOB_TRACKER("job_tracker"), FORMULA_DETAIL("formula_detail"), EVENT_TIMELINE("event_timeline"), EVENT_DETAIL("event_detail"), JOB_CONFIG_SETTING("job_config_settings");

		private String columnFamily;

		private columnFamily(String value) {
			columnFamily = value;
		}

		public String columnFamily() {
			return columnFamily;
		}
	}
	
	public enum dataTypes{
		STRING("string"), INTEGER("integer"), LONG("long"), BOOLEAN("boolean");
		
		private String dataType;
		
		private dataTypes(String value){
			dataType = value;
		}
		
		public String dataType(){
			return dataType;
		}
	}
}
