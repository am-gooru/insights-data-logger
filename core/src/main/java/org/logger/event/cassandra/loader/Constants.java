package org.logger.event.cassandra.loader;

public interface Constants {
	
	public static final String FIRST_SESSION = "FS~";
    
    public static final  String RECENT_SESSION = "RS~";
    
    public static final  String RS= "RS";
    
    public static final  String AS= "AS";
    
    public static final  String ALL_SESSION = "AS~";
    
    public static final  String SEPERATOR = "~";
    
    public static final  String COMMA = ",";
    
    public static final  String EMPTY = "";
    
	public static final  String CONTEXT = "context";
	
	public static final  String _PAYLOAD_OBJECT = "pay_load_object";
	
	public static final  String USER = "user";
	
	public static final  String USER_IP = "userIp";
	
	public static final  String PAY_LOAD = "payLoadObject";
	
	public static final  String METRICS = "metrics";
	
	public static final  String LAST_ACCESSED_USER_UID = "last_accessed_useruid";
	
	public static final  String _LAST_ACCESSED_USER = "last_accessed_user";
	
	public static final  String LAST_ACCESSED_USER = "lastAccessedUser";
	
	public static final  String _LAST_ACCESSED = "last_accessed";
	
	public static final  String LAST_ACCESSED = "lastAccessed";
	
	public static final  String EVENT_NAME = "eventName";
	
	public static final  String CONTENT_GOORU_OID = "contentGooruId";
	
	public static final  String SOURCE_GOORU_OID = "sourceGooruId";
	
	public static final  String PARENT_GOORU_OID = "parentGooruId";
	
	public static final  String CLASS_GOORU_OID = "classGooruId";
	
	public static final  String UNIT_GOORU_OID = "unitGooruId";
	
	public static final  String LESSON_GOORU_OID = "lessonGooruId";
	
	public static final  String COURSE_GOORU_OID = "courseGooruId";
	
	public static final  String TIMEINMSEC = "timeInMillSec";
	
	public static final  String TOTALTIMEINMS = "totalTimeSpentInMs";
	
	public static final  String TIMESPENTINMS = "timeSpentInMs";
	
	public static final  String AVG_TIME_SPENT = "avg_time_spent";
	
	public static final  String TIME_SPENT = "time_spent";
	
	public static final  String _COLLECTION_TYPE = "collection_type";
	
	public static final  String COLLECTION_TYPE = "collectionType";
	
	public static final  String EVIDENCE = "evidence";
	
	public static final  String _EVENT_ID = "event_id";
	
	public static final  String EVENT_ID = "eventId";
	
	public static final  String EVENT_TIME = "eventTime";
	
	public static final  String _EVENT_TIME = "event_time";
	
	public static final  String PARENT_EVENT_ID = "parentEventId";
	
	public static final  String _PARENT_EVENT_ID = "parent_event_id";
	
	//public static final  String CLASSPAGEGOORUOID= "classPageGooruId";
	
	public static final  String CLASSPAGEGOORUOID= "classpageGooruId";
	
	public static final  String VERSION = "version";

	public static final  String EVENT_TYPE = "eventType";
	
	public static final  String SESSION_ID = "sessionId";
	
	public static final  String SESSION = "session";
	
	public static final  String _FIRST_ATTEMPT_STATUS = "first_attempt_status";
	
	public static final  String FIRST_ATTEMPT_STATUS = "firstAttemptStatus";
	
	public static final  String _ANSWER_STATUS = "answer_status";
	
	public static final  String ANSWERSTATUS = "answerStatus";
	
	public static final  String _SESSION_ID = "session_id";
	
	public static final  String SESSION_TOKEN = "sessionToken";
	
	public static final  String STAT_FIELDS = "statFields";
	
	public static final  String _BATCH_SIZE = "index~loop~count";
	
	public static final  String GOORUID = "gooruUId" ;
	
	public static final  String AGG = "aggregator";
	
	public static final  String AGGTYPE = "aggregatorType";
	
	public static final  String AGGMODE = "aggregatorMode";
	
	public static final  String SUM = "sum";
	
	public static final  String SUB = "sub";
	
	public static final  String AVG = "avg";
	
	public static final  String SUMOFAVG = "sumofavg";
	
	public static final  String DIVIDEND = "dividend";
	
	public static final  String DIVISOR = "divisor" ;

	public static final  String CHOICE = "choice";
	
	public static final  String _QUESTION_STATUS = "question_status";
	
	public static final  String _ANSWER_OBECT = "answer_object";
	
	public static final  String ANSWER_OBECT = "answerObject";
	
	public static final  String ATTEMPTS = "attempts";
	
	public static final  String OPTIONS = "options";
	
	public static final  String COUNTER = "counter";
	
	public static final  String REACTION_TYPE = "reactionType";
	
	public static final  String REACTION = "reaction";
	
	public static final  String TOTAL_REACTION = "total_reaction";
	
	public static final  String REACTED_COUNT = "reacted_count";
	
	public static final  String RESOURCE_TYPE = "resourceType";
	
	public static final  String QUESTION_TYPE = "questionType";

	public static final  String OE = "OE";
	
	public static final  String TYPE = "type";
	
	public static final  String ITEM_TYPE = "itemType";
	
	public static final  String COLLECTION_ITEM_TYPE = "collectionItemType";
	
	public static final  String _ITEM_TYPE = "item_type";
	
	public static final  String ACTION_TYPE = "actionType";
	
	public static final  String USER_AGENT = "userAgent";
	
	public static final  String REGISTER_TYPE = "registerType";
	
	public static final  String CLIENT_SOURCE = "clientSource";
	
	public static final  String AUTO = "auto";
	
	public static final  String STOP = "stop";
	
	public static final  String START = "start";
	
	public static final  String COLLECTION_ITEM_START = "collectionItemStart";
	
	public static final  String COLLECTION_ITEM_STOP = "collectionItemStop";
	
	public static final  String ANS = "answers";
	
	public static final  String TEXT = "text";
	
	public static final  String _FEEDBACK = "feed_back";
	
	public static final  String _FEED_BACK_PROVIDER = "feed_back_provider";
	
	public static final  String PROVIDER = "feedbackProviderUId";
	
	public static final  String _FEED_BACK_TIMESTAMP = "feed_back_timestamp";
	
	public static final  String _RESOURCE_CONTENT_ID = "resource_content_id";
	
	public static final  String RESOURCE_CONTENT_ID = "resourceContentId";
	
	public static final  String VIEWS = "views";
	
	public static final  String VIEWS_COUNT = "viewsCount";
	
	public static final  String ATTEMPT_COUNT = "attemptCount";
	
	public static final  String COMPLETED = "completed";
	
	public static final  String COMPLETED_EVENT = "completed-event";
	
	public static final  String INPROGRESS = "in-progress";
	
	public static final  String INDEX_WAITING_LIMIT = "index~waiting~limit";
	
	public static final  String INDEX_WAITING_COUNT = "index~waiting~count";
	
	public static final  String INDEXED_LIMIT = "indexed~limit";
	
	public static final  String INDEX_LIMIT = "index~limit";
	
	public static final  String INDEX_STATUS = "search~index~status";
	
	public static final  String CURR_INDEXING_LIMIT = "curr~indexing~limit";
	
	public static final  String VIEWS_LAST_UPDATED = "view~count~last~updated";
	
	public static final  String VIEW_EVENTS = "viewEvents";
	
	public static final  String ATMOSPHERE_END_POINT = "atmosphereEndPoint";
	
	public static final  String VIEW_UPDATE_END_POINT = "viewCountAPI";
	
	public static final  String SEARCH_INDEX_API = "searchIndexAPI";
	
	public static final  String QUESTION = "question";

	public static final  String ATTMPT_TRY_SEQ = "attemptTrySequence";
	
	public static final  String ATTMPT_STATUS = "attemptStatus";
	
	public static final  String COLLECTION = "collection";
	
	public static final  String ASSESSMENT = "assessment";
	
	public static final  String ASSESSMENT_URL = "assessment/url";
	
	public static final  String ASSESSMENT_COUNT = "assessmentCount";
	
	public static final  String ANSWER_ID = "answerId";
	
	public static final  String OPEN_ENDED_TEXT = "openEndedText";
	
	public static final  String RESOURCE = "resource";
	
	public static final  String CONTEXT_INFO = "contextInfo";
	
	public static final  String MOBILE_DATA = "mobileData";
	
	public static final  String HINT_ID = "hintId";
	
	public static final  String COLLABORATOR_IDS = "collaboratorIds";
	
	public static final  String _GOORU_OID = "gooru_oid";
	
	public static final  String _GOORU_UID = "gooru_uid";
	
	public static final  String _USER_UID = "user_uid";
	
	public static final  String USER_UID = "userUid";
	
	public static final  String USERNAME = "username";
	
	public static final  String _CLASSPAGEID = "classpage_id";
	
	public static final  String MINUTE_DATE_FORMATTER = "yyyyMMddkkmm";

	public static final  String FIELDS = "fields";
	
	public static final  String INFO = "info";
	
	public static final  String _START_TIME = "start_time";
	
	public static final  String START_TIME = "startTime";
	
	public static final  String END_TIME = "endTime";
	
	public static final  String _END_TIME = "end_time";
	
	public static final  String API_KEY = "apiKey";
	
	public static final  String _API_KEY = "api_key";
	
	public static final  String ORGANIZATION_UID = "organizationUId";
	
	public static final  String _EVENT_VALUE = "event_value";
	
	public static final  String _TIME_SPENT_IN_MILLIS = "time_spent_in_millis";
	
	public static final  String _CONTENT_GOORU_OID = "content_gooru_oid";

	public static final  String RESOURCE_ID = "resourceId";
	
	public static final  String _PARENT_GOORU_OID = "parent_gooru_oid";
	
	public static final  String PARENT_ID = "parentId";
	
	public static final  String _EVENT_TYPE = "event_type";
	
	public static final  String _EVENT_NAME = "event_name";
	
	public static final  String MODE = "mode";

	public static final  String STUDY = "study";
	
	public static final  String PREVIEW = "preview";
	
	public static final  String _USER_GROUP_UID =	"user_group_uid"; 
	
	public static final  String _CLASSPAGE_CONTENT_ID = "classpage_content_id";
	
	public static final  String _CLASSPAGE_CODE = "classpage_code";				
	
	public static final  String _USER_GROUP_CODE = "user_group_code";
	
	public static final  String _CLASSPAGE_GOORU_OID = "classpage_gooru_oid"; 
	
	public static final  String _ORGANIZATION_UID = "organization_uid";
	
	public static final  String RAW_UPDATE = "rawupdate";
	
	public static final  String GROUP_UID = "groupUId";
	
	public static final  String CLASS_CODE = "classCode";
	
	public static final  String CONTENT_ID = "contentId";
	
	public static final  String _CONTENT_ID = "content_id";
	
	public static final  String COLLECTION_ITEM_ID = "collectionItemId";
	
	public static final  String _COLLECTION_ITEM_ID = "collection_item_id";
	
	public static final  String _COLLECTION_CONTENT_ID = "collection_content_id";
	
	public static final  String COLLECTION_CONTENT_ID = "collectionContentId";
	
	public static final  String PARENT_CONTENT_ID = "parentContentId";
	
	public static final  String _PARENT_CONTENT_ID = "parent_content_id";
	
	public static final  String _MINIMUM_SCORE = "minimum_score";
	
	public static final  String MINIMUM_SCORE = "minimumScore";
	
	public static final  String COLLECTION_ITEM_MINIMUM_SCORE = "collectionItemMinimumScore";
	
	public static final  String NARRATION = "narration";
	
	public static final  String COLLECTION_ITEM_NARRATION = "collectionItemNarration";
	
	public static final  String _ESTIMATED_TIME = "estimated_time";
	
	public static final  String ESTIMATED_TIME = "estimatedTime";
	
	public static final  String COLLECTION_ITEM_ESTIMATED_TIME = "collectionItemEstimatedTime";
	
	public static final  String _QUESTION_TYPE = "question_type";	
	
	public static final  String COLLECTION_ITEM_QUESTION_TYPE = "collectionItemQuestionType";
	
	public static final  String ITEM_SEQUENCE = "itemSequence";
	
	public static final  String _ITEM_SEQUENCE = "item_sequence";

	public static final  String COLLECTION_ITEM_SEQUENCE = "collectionItemSequence";
	
	public static final  String _COLLECTION_GOORU_OID = "collection_gooru_oid";
	
	public static final  String COLLECTION_GOORU_OID = "collectionGooruOid";
	
	public static final  String _RESOURCE_GOORU_OID = "resource_gooru_oid";

	public static final  String RESOURCE_GOORU_OID = "resourceGooruOid";
	
	public static final  String SCORE = "score";
	
	public static final  String TOTAL_SCORE = "score";
	
	public static final  String _SCORE_IN_PERCENTAGE = "score_in_percentage";
	
	public static final  String SCORE_IN_PERCENTAGE = "scoreInPercentage";
	
	public static final  String TOTAL_QUESTIONS_COUNT = "totalQuestionsCount";
	
	public static final  String _QUESTION_COUNT = "question_count";
	
	public static final  String _GRADE_IN_PERCENTAGE = "grade_in_percentage";
	
	public static final  String ACTIVE = "active";
	
	public static final  String COUNT = "count";
	
	public static final  String _TIME_SPENT = "time_spent";
	
	public static final  String DIFF = "diff";
	
	public static final  String DEFAULT_ORGANIZATION_UID = "4261739e-ccae-11e1-adfb-5404a609bd14";
	
	public static final  String DEFAULT_API_KEY = "b6b82f4d-0e6e-4ad5-96d9-30849cf17727";
	
	public static final  String DEFAULT_COLUMN = "constant_value";
	
	public static final  String DEFAULTKEY = "default~key";
	
	public static final  String STAT_METRICS = "stat~metrics";
	
	public static final  String SCH_STATUS = "schdulers~status";
	
	public static final  String SCH_HOST = "schedular~host";
	
	public static final  String HOST = "host";
	
	public static final  String REAL_TIME_INDEXING = "real~time~indexing";
	
	public static final  String _VIEW_EVENTS = "views~events";
	
	public static final  String ATM_END_POINT = "atmosphere.end.point";
	
	public static final  String ACTIVITY_INDEX_MAX_COUNT = "activity~indexing~max~count";
	
	public static final  String ACTIVITY_INDEX_CHECKED_COUNT = "activity~indexing~checked~count";
	
	public static final  String ACTIVITY_INDEX_LAST_UPDATED = "activity~indexing~last~updated";
	
	public static final  String ACTIVITY_INDEX_STATUS = "activity~indexing~status";
	
	public static final  String USER_SESSION = "user~session";
	
	public static final  String AGG_JSON = "aggregator_json";
	
	public static final  String ANONYMOUS_SESSION = "anonymous~session";
	
	public static final  String ANONYMOUS = "ANONYMOUS";
	
	public static final  String ALL_USER_SESSION = "all~user~session";
	
	public static final  String ACTIVE_SESSION = "active~session";
	
	public static final  String ACTIVE_COLLECTION_PLAYS = "active~collection~plays";
	
	public static final  String ACTIVE_RESOURCE_PLAYS = "active~resource~plays";
	
	public static final  String ACTIVE_COLLECTION_RESOURCE_PLAYS = "active~collection~resource~plays";
	
	public static final  String ACTIVE_PLAYS = "active~content~plays";
	
	public static final  String CONFIG_SETTINGS = "config_settings";
	
	public static final  String DATA = "data";
	
	public static final  String SYSTEM = "System";
	
	public static final  String NAME = "name";
	
	public static final  String EDIT = "edit";
	
	public static final  String CREATE = "create";
	
	public static final  String COPY = "copy";
	
	public static final  String MOVE = "move";
	
	public static final  String ITEMTYPES_SCSFFC =  "shelf.collection|shelf.folder|folder.collection";
	
	public static final  String RESOURCETYPES_SF =  "scollection|folder";
	
	public static final  String COLLECTION_TABLE_FIELDS = "contentId,gooruOid,collectionType,grade,goals,ideas,performanceTasks,language,keyPoints,notes,languageObjective,network,mailNotification,buildTypeId,narrationLink,estimatedTime";
	
	public static final  String COLLECTION_ITEM_FIELDS = "collectionItemId|itemSequence|itemType";
	
	public static final  String CLASSPAGE = "classpage";
	
	public static final  String INDEXING_VERSION = "index_version";
	
	public static final  String INDEX_EVENTS = ".*collection.play.*|.*collection.resource.play.*|.*resource.play.*|.*reaction.create.*|.*item.rate.*|.*item.flag.*|.*item.create.*";
	
	public static final  String V2_KAFKA_MICRO_PRODUCER = "v2~kafka~microaggregator~producer";
	
	public static final  String V2_KAFKA_LOG_WRITER_PRODUCER = "v2~kafka~logwritter~producer";
	
	public static final  String V2_KAFKA_CONSUMER = "v2~kafka~consumer";
	
	public static final  String V2_KAFKA_LOG_WRITER_CONSUMER = "v2~kafka~logwritter~consumer";
	
	public static final  String V2_KAFKA_MICRO_CONSUMER = "v2~kafka~microaggregator~consumer";
	
	public static final  String KAFKA_IP =  "kafka_ip";
	
	public static final  String KAFKA_PORT = "kafka_portno";
	
	public static final  String KAFKA_TOPIC = "kafka_topic";
	
	public static final  String KAFKA_PRODUCER_TYPE = "kafka_producertype";

	public static final int RECURSION_MAX_DEPTH = 8;
	
	public static final  String SCOLLECTION = "scollection";

	public static final String READ_EXCEPTION = "Exception while reading ColumnFamily:";
	
	public static final String WRITE_EXCEPTION = "Exception while writing ColumnFamily:";
	
	public static final String DELETE_EXCEPTION = "Exception while deleting ColumnFamily:";

	public static final String ASSOCIATION_DATE = "associationDate";
	
	public static final String ITEM_ID = "itemId";
	
	public static final String DELETED = "deleted";
	
	public static final String SOURCE_ITEM_ID = "sourceItemId";
	
	public static final String GOORU_OID = "gooruOid";
	
	public static final String _USER_GROUP_TYPE = "user_group_type";
	
	public static final String USER_GROUP_TYPE = "userGroupType";
	
	public static final  String CLASS_ID = "classId";
		
	public static final  String ACTIVE_FLAG = "activeFlag";

	public static final  String _ACTIVE_FLAG = "active_flag";
		
	public static final String IS_GROUP_OWNER = "isGroupOwner";

	public static final String _IS_GROUP_OWNER = "is_group_owner";

	public static final String _NARRATION_TYPE = "narration_type";
	
	public static final String NARRATION_TYPE = "narrationType";
	
	public static final String COLLECTION_NARRATION_TYPE = "collectionItemNarrationType";
	
	public static final String _PLANNED_END_DATE = "planned_end_date";
	
	public static final String PLANNED_END_DATE = "plannedEndDate";
	
	public static final String COLLECTION_ITEM_PLANNED_END_DATE = "collectionItemPlannedEndDate";
	
	public static final String _ASSOCIATION_DATE = "association_date";
	
	public static final String COLLECTION_ITEM_ASSOCIATION_DATE = "collectionItemAssociationDate";
	
	public static final String COLLECTION_ITEM_ASSOCIATE_BY_UID = "collectionItemAssociatedByUid";
	
	public static final String _ASSOCIATED_BY_UID = "associated_by_uid";
	
	public static final String ASSOCIATED_BY_UID = "associatedByUid";
	
	public static final String _IS_REQUIRED = "is_required";
	
	public static final String IS_REQUIRED = "isRequired";
	
	public static final String COLLECTION_ITEM_IS_REQUIRED = "collectionItemIsRequired";
	
	public static final String EVENT_SOURCE = "api-logged";
	
	public static final String INVALID_API_KEY = "Oops! Invalid API Key.";
	
	public static final String INVALID_JSON = "Oops! Invalid JSON.";
	
	public static final String BAD_REQUEST = "Oops! You have to send event json.";
	
	public static final String FORWARD_SLASH = "\"";
	
	public static final String EMPTY_STRING = "";
	
	public static final String EMPTY_EXCEPTION = " must not be Emtpy!";
	
	public static final String QUESTION_COUNT = "questionCount";
	
	public static final String RESOURCE_COUNT = "resourceCount";
	
	public static final String OE_COUNT = "oeCount";
	
	public static final String MC_COUNT = "mcCount";
	
	public static final String FIB_COUNT = "fibCount";
	
	public static final String MA_COUNT = "maCount";
	
	public static final String TF_COUNT = "tfCount";
	
	public static final String ITEM_COUNT = "itemCount";
	
	public static final String INDEXINGVERSION = "index_version";	
	
	public static final String MONITOR_KAFKA_CONSUMER = "monitor~kafka~consumer";
	
	public static final String STATUS = "status";
	
	public static final String MAIL_LOOP_COUNT = "mail_loop_count";
	
	public static final String THREAD_LOOP_COUNT = "thread_loop_count";
	
	public static final String THREAD_SLEEP_TIME = "thread_sleep_time";
	
	public static final String PATHWAYGOORUOID= "pathwayGooruId";

	public static final String CLASS_NOT_FOUND= "classpage id is null";
	
	public static final String TAXONOMY_IDS = "tax_ids";
	
	public static final String SUBJECT = "subject";
	
	public static final String COURSE = "course";
	
	public static final String GRADE = "grade";
	
	public static final String GRADE_FIELD = "grade1";
	
	public static final String SUBJECT_CODE_ID = "subject_code_id";
	
	public static final String COURSE_CODE_ID = "course_code_id";
	
	public static final String HIT_COUNT = "hitCount";
	
	public static final String RESULT_COUNT = "resultCount";
	
	public static final String REGION = "region";
	
	public static final String LATITUDE = "latitude";
	
	public static final String LONGITUDE = "longitude";
	
	public static final String CITY = "city";
	
	public static final String DEFAULTCOLUMN = "constant_value";
	
	public static final String PREVIOUS_RATE = "previousRate";
	
	public static final String COUNT_SEPARATOR_RATINGS = "count~ratings";
	
	public static final String RATE = "rate";
	
}
