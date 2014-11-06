package org.kafka.event.microaggregator.core;

public interface Constants {
	
	public String FIRSTSESSION = "FS~";
    
    public String RECENTSESSION = "RS~";
    
    public String ALLSESSION = "AS~";
    
    public String SEPERATOR = "~";
    
	public String CONTEXT = "context";
	
	public String USER = "user";
	
	public String PAYLOAD = "payLoadObject";
	
	public String METRICS = "metrics";
	
	public String CONFIG_SETTINGS = "config_settings";
	
	public String LASTACCESSEDUSERUID = "last~accessed~useruid";
	
	public String LASTACCESSEDUSER = "last~accessed~user";
	
	public String LASTACCESSED = "last~accessed";
	
	public String EVENTNAME = "eventName";
	
	public String CONTENTGOORUOID = "contentGooruId";
	
	public String PARENTGOORUOID = "parentGooruId";
	
	public String TIMEINMS = "timeInMillSec";
	
	public String TOTALTIMEINMS = "totalTimeSpentInMs";
	
	public String EVENTIS = "eventId";
	
	public String PARENTEVENTID = "parentEventId";
	
	public String PARENT_EVENT_ID = "parent_event_id";
	
	//public String CLASSPAGEGOORUOID= "classPageGooruId";
	
	public String CLASSPAGEGOORUOID= "classpageGooruId";
	
	public String VERSION = "version";

	public String EVENTTYPE = "eventType";
	
	public String SESSION = "sessionId";
	
	public String GOORUID = "gooruUId" ;
	
	public String AGG = "aggregator";
	
	public String AGGTYPE = "aggregatorType";
	
	public String AGGMODE = "aggregatorMode";
	
	public String SUM = "sum";
	
	public String AVG = "avg";
	
	public String SUMOFAVG = "sumofavg";
	
	public String DIVIDEND = "dividend";
	
	public String DIVISOR = "divisor" ;

	public String CHOICE = "choice";
	
	public String STATUS = "question_status";
	
	public String ANSWER_OBECT = "answer_object";
	
	public String ANSWEROBECT = "answerObject";
	
	public String ATTEMPTS = "attempts";
	
	public String OPTIONS = "options";
	
	public String COUNTER = "counter";
	
	public String RESOURCETYPE = "resourceType";
	
	public String QUESTIONTYPE = "questionType";

	public String OE = "OE";
	
	public String TYPE = "type";
	
	public String ITEMTYPE = "itemType";
	
	public String REGISTERTYPE = "registerType";
	
	public String CLIENTSOURCE = "clientSource";
	
	public String AUTO = "auto";
	
	public String STOP = "stop";
	
	public String START = "start";
	
	public String ANS = "answers";
	
	public String TEXT = "text";
	
	public String FEEDBACK = "feed_back";
	
	public String FEEDBACKPROVIDER = "feed_back_provider";
	
	public String PROVIDER = "feedbackProviderUId";
	
	public String TIMESTAMP = "feed_back_timestamp";
	
	public String VIEWS = "views";
	
	public String QUESTION = "question";

	public String ATTMPTTRYSEQ = "attemptTrySequence";
	
	public String ATTMPTSTATUS = "attemptStatus";
	
	public String COLLECTION = "collection";
	
	public String RESOURCE = "resource";
	
	public String GOORUOID = "gooru_oid";
	
	public String USERID = "gooru_uid";
	
	public String CLASSPAGEID = "classpage_id";
	
	public String MINUTEDATEFORMATTER = "yyyyMMddkkmm";

	public String FIELDS = "fields";
		
	public String START_TIME = "start_time";
	
	public String STARTTIME = "startTime";
	
	public String END_TIME = "end_time";
	
	public String APIKEY = "apiKey";
	
	public String ORGANIZATIONUID = "organizationUId";
	
	public String EVENT_VALUE = "event_value";
	
	public String CONTENT_GOORU_OID = "content_gooru_oid";

	public String PARENT_GOORU_OID = "parent_gooru_oid";
	
	public String EVENT_TYPE = "event_type";
	
	public String EVENT_NAME = "event_name";
	
	public String MODE = "mode";

	public String STUDY = "study";
	
	public String PREVIEW = "preview";
	
	public String USER_GROUP_UID =	"user_group_uid"; 
	
	public String CLASSPAGE_CONTENT_ID = "classpage_content_id";
	
	public String CLASSPAGE_CODE = "classpage_code";				
	
	public String USER_GROUP_CODE = "user_group_code";
	
	public String CLASSPAGE_GOORU_OID = "classpage_gooru_oid"; 
	
	public String ORGANIZATION_UID = "organization_uid";
	
	public String RAWUPDATE = "rawupdate";
	
	public String GROUPUID = "groupUId";
	
	public String CLASSCODE = "classCode";
	
	public String CONTENTID = "contentId";
	
	public String CONTENT_ID = "content_id";
	
	public String COLLECTIONITEMID = "collectionItemId";
	
	public String COLLECTION_ITEM_ID = "collection_item_id";
	
	public String PARENTCONTENTID = "parentContentId";
	
	public String PARENT_CONTENT_ID = "parent_content_id";
	
	public String ITEMSEQUENCE = "itemSequence";
	
	public String ITEM_SEQUENCE = "item_sequence";

	public String COLLECTION_GOORU_OID = "collection_gooru_oid";
	
	public String RESOURCE_GOORU_OID = "resource_gooru_oid";

	public String SCORE = "score";
	
	public String QUESTION_COUNT = "question_count";
	
	public String SCORE_IN_PERCENTAGE = "grade_in_percentage";
	
	public String ACTIVE = "active";
	
	public String COUNT = "count";
	
	public String DIFF = "diff";
	
	public String DEFAULT_ORGANIZATION_UID = "4261739e-ccae-11e1-adfb-5404a609bd14";
	
	public String CONSTANT_VALUE = "constant_value";
	
	public String LAST_PROCESSED_TIME = "last_processed_time";
	
	public String EVENT_TIMELINE_KEY_FORMAT ="yyyyMMddkkmm";
	
	public String MINUTE_AGGREGATOR_PROCESSOR_KEY="aggregation~events";
	
	public String FORMULA ="formula";
	
	public String COMMA =",";
	
	public String ITEM_VALUE = "item_value";
	
	public String EMPTY ="";
	
	public String DOUBLE_QUOTES ="\"";
	
	public String AVOID_SPECIAL_CHARACTER ="[^a-zA-Z0-9]";
	
	
	public enum sortType{
		
		ASC("ASC"),DESC("DESC");
		
		private String sortOrder;
		
		private sortType(String name){
			sortOrder = name;
		}
		
		public String sortType(){
			return sortOrder;
		}
	}
	public enum eventJSON{
		
		SESSION("session"),CONTEXT("context"),PAYLOADOBJECT("payLoadObject"),USER("user");
		
		private String eventJSON;
		
		private eventJSON(String value){
			eventJSON = value;
		}
		
		public String eventJSON(){
			return eventJSON;
		}
		
	}
	
	public enum formulaDetail{
		
		NAME("name"),FORMULA("formula"),FORMULAS("formulas"),COMPLETED("completed"),INPROGRESS("inprogress"),ACTIVE("active"),EVENTS("events"),STATUS("status"),REQUEST_VALUES("requestValues");
		
		private String formulaKey;
		
		private formulaDetail(String value){
			formulaKey = value;
		}
		
		public String formulaDetail(){
			return formulaKey;
		}
	}
	
	public enum columnKey{
		C("C:"),D("D:"),E("E:");
		
		private String column;
		
		private columnKey(String name){
			column = name;
		}
		public String columnKey(){
			return column;
		}
	}
	
	public enum mapKey{
		
		KEY("key"),VALUE("value");
		
		private String map;
		
		private mapKey(String value){
			map = value;
		}
		
		public String mapKey(){
			return map;
		}
		
	}
	
	public enum aggregateType{
	
		KEY("aggregate_type"),NORMAL("normal"),AGGREGATE("aggregate");
		
		private String aggregateName;
		
		private aggregateType(String value){
			aggregateName = value;
		}
		
		public String aggregateType(){
			return aggregateName;
		}
		
	}
	
	public enum columnFamily{
		
		LIVE_DASHBOARD("live_dashboard"),AGGREGATION_DETAIL("aggregation_detail"),FORMULA_DETAIL("formula_detail"),EVENT_TIMELINE("event_timeline"),
		EVENT_DETAIL("event_detail"),JOB_CONFIG_SETTING("job_config_settings");
		
		private String columnFamily;
		
		private columnFamily(String value){
			columnFamily = value;
		}
		
		public String columnFamily(){
		return columnFamily;	
		}
	}
}
