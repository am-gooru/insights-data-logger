package org.logger.event.cassandra.loader;

public interface Constants {
	
	public static final String FIRSTSESSION = "FS~";
    
    public static final  String RECENTSESSION = "RS~";
    
    public static final  String ALLSESSION = "AS~";
    
    public static final  String SEPERATOR = "~";
    
	public static final  String CONTEXT = "context";
	
	public static final  String USER = "user";
	
	public static final  String PAYLOAD = "payLoadObject";
	
	public static final  String METRICS = "metrics";
	
	public static final  String LASTACCESSEDUSERUID = "last~accessed~useruid";
	
	public static final  String LASTACCESSEDUSER = "last~accessed~user";
	
	public static final  String LASTACCESSED = "last~accessed";
	
	public static final  String EVENTNAME = "eventName";
	
	public static final  String CONTENTGOORUOID = "contentGooruId";
	
	public static final  String SOURCEGOORUOID = "sourceGooruId";
	
	public static final  String PARENTGOORUOID = "parentGooruId";
	
	public static final  String TIMEINMS = "timeInMillSec";
	
	public static final  String TOTALTIMEINMS = "totalTimeSpentInMs";
	
	public static final  String EVENT_ID = "event_id";
	
	public static final  String EVENTID = "eventId";
	
	public static final  String PARENTEVENTID = "parentEventId";
	
	public static final  String PARENT_EVENT_ID = "parent_event_id";
	
	//public static final  String CLASSPAGEGOORUOID= "classPageGooruId";
	
	public static final  String CLASSPAGEGOORUOID= "classpageGooruId";
	
	public static final  String VERSION = "version";

	public static final  String EVENTTYPE = "eventType";
	
	public static final  String SESSION = "sessionId";
	
	public static final  String SESSIONTOKEN = "sessionToken";
	
	public static final  String STATFIELDS = "statFields";
	
	public static final  String BATCH_SIZE = "index~loop~count";
	
	public static final  String GOORUID = "gooruUId" ;
	
	public static final  String AGG = "aggregator";
	
	public static final  String AGGTYPE = "aggregatorType";
	
	public static final  String AGGMODE = "aggregatorMode";
	
	public static final  String SUM = "sum";
	
	public static final  String AVG = "avg";
	
	public static final  String SUMOFAVG = "sumofavg";
	
	public static final  String DIVIDEND = "dividend";
	
	public static final  String DIVISOR = "divisor" ;

	public static final  String CHOICE = "choice";
	
	public static final  String STATUS = "question_status";
	
	public static final  String ANSWER_OBECT = "answer_object";
	
	public static final  String ANSWEROBECT = "answerObject";
	
	public static final  String ATTEMPTS = "attempts";
	
	public static final  String OPTIONS = "options";
	
	public static final  String COUNTER = "counter";
	
	public static final  String RESOURCETYPE = "resourceType";
	
	public static final  String QUESTIONTYPE = "questionType";

	public static final  String OE = "OE";
	
	public static final  String TYPE = "type";
	
	public static final  String ITEMTYPE = "itemType";
	
	public static final  String ACTIONTYPE = "actionType";
	
	public static final  String USERAGENT = "userAgent";
	
	public static final  String REGISTERTYPE = "registerType";
	
	public static final  String CLIENTSOURCE = "clientSource";
	
	public static final  String AUTO = "auto";
	
	public static final  String STOP = "stop";
	
	public static final  String START = "start";
	
	public static final  String ANS = "answers";
	
	public static final  String TEXT = "text";
	
	public static final  String FEEDBACK = "feed_back";
	
	public static final  String FEEDBACKPROVIDER = "feed_back_provider";
	
	public static final  String PROVIDER = "feedbackProviderUId";
	
	public static final  String TIMESTAMP = "feed_back_timestamp";
	
	public static final  String VIEWS = "views";
	
	public static final  String COMPLETED = "completed";
	
	public static final  String INPROGRESS = "in-progress";
	
	public static final  String INDEXWAITINGLIMIT = "index~waiting~limit";
	
	public static final  String INDEXWAITINGCOUNT = "index~waiting~count";
	
	public static final  String INDEXEDLIMIT = "indexed~limit";
	
	public static final  String INDEXLIMIT = "index~limit";
	
	public static final  String INDEXSTATUS = "search~index~status";
	
	public static final  String CURRINDEXINGLIMIT = "curr~indexing~limit";
	
	public static final  String VIEWSLASTUPDATED = "view~count~last~updated";
	
	public static final  String VIEWEVENTS = "viewEvents";
	
	public static final  String ATMOSPHERENDPOINT = "atmosphereEndPoint";
	
	public static final  String VIEWUPDATEENDPOINT = "viewCountAPI";
	
	public static final  String SEARCHINDEXAPI = "searchIndexAPI";
	
	public static final  String QUESTION = "question";

	public static final  String ATTMPTTRYSEQ = "attemptTrySequence";
	
	public static final  String ATTMPTSTATUS = "attemptStatus";
	
	public static final  String COLLECTION = "collection";
	
	public static final  String RESOURCE = "resource";
	
	public static final  String GOORUOID = "gooru_oid";
	
	public static final  String USERID = "gooru_uid";
	
	public static final  String CLASSPAGEID = "classpage_id";
	
	public static final  String MINUTEDATEFORMATTER = "yyyyMMddkkmm";

	public static final  String FIELDS = "fields";
		
	public static final  String START_TIME = "start_time";
	
	public static final  String STARTTIME = "startTime";
	
	public static final  String END_TIME = "end_time";
	
	public static final  String APIKEY = "apiKey";
	
	public static final  String API_KEY = "api_key";
	
	public static final  String ORGANIZATIONUID = "organizationUId";
	
	public static final  String EVENT_VALUE = "event_value";
	
	public static final  String CONTENT_GOORU_OID = "content_gooru_oid";

	public static final  String PARENT_GOORU_OID = "parent_gooru_oid";
	
	public static final  String EVENT_TYPE = "event_type";
	
	public static final  String EVENT_NAME = "event_name";
	
	public static final  String MODE = "mode";

	public static final  String STUDY = "study";
	
	public static final  String PREVIEW = "preview";
	
	public static final  String USER_GROUP_UID =	"user_group_uid"; 
	
	public static final  String CLASSPAGE_CONTENT_ID = "classpage_content_id";
	
	public static final  String CLASSPAGE_CODE = "classpage_code";				
	
	public static final  String USER_GROUP_CODE = "user_group_code";
	
	public static final  String CLASSPAGE_GOORU_OID = "classpage_gooru_oid"; 
	
	public static final  String ORGANIZATION_UID = "organization_uid";
	
	public static final  String RAWUPDATE = "rawupdate";
	
	public static final  String GROUPUID = "groupUId";
	
	public static final  String CLASSCODE = "classCode";
	
	public static final  String CONTENTID = "contentId";
	
	public static final  String CONTENT_ID = "content_id";
	
	public static final  String COLLECTIONITEMID = "collectionItemId";
	
	public static final  String COLLECTION_ITEM_ID = "collection_item_id";
	
	public static final  String PARENTCONTENTID = "parentContentId";
	
	public static final  String PARENT_CONTENT_ID = "parent_content_id";
	
	public static final  String ITEMSEQUENCE = "itemSequence";
	
	public static final  String ITEM_SEQUENCE = "item_sequence";

	public static final  String COLLECTION_GOORU_OID = "collection_gooru_oid";
	
	public static final  String RESOURCE_GOORU_OID = "resource_gooru_oid";

	public static final  String SCORE = "score";
	
	public static final  String QUESTION_COUNT = "question_count";
	
	public static final  String SCORE_IN_PERCENTAGE = "grade_in_percentage";
	
	public static final  String ACTIVE = "active";
	
	public static final  String COUNT = "count";
	
	public static final  String TIMESPENT = "time_spent";
	
	public static final  String DIFF = "diff";
	
	public static final  String DEFAULT_ORGANIZATION_UID = "4261739e-ccae-11e1-adfb-5404a609bd14";
	
	public static final  String DEFAULT_API_KEY = "b6b82f4d-0e6e-4ad5-96d9-30849cf17727";
	
	public static final  String DEFAULTCOLUMN = "constant_value";
	
	public static final  String DEFAULTKEY = "default~key";
	
	public static final  String STATMETRICS = "stat~metrics";
	
	public static final  String SCHSTATUS = "schdulers~status";
	
	public static final  String SCHHOST = "schedular~host";
	
	public static final  String HOST = "host";
	
	public static final  String REALTIMEINDEXING = "real~time~indexing";
	
	public static final  String VIEW_EVENTS = "views~events";
	
	public static final  String ATMOENDPOINT = "atmosphere.end.point";
	
	public static final  String ACTIVITYINDEXMAXCOUNT = "activity~indexing~max~count";
	
	public static final  String ACTIVITYINDEXCHECKEDCOUNT = "activity~indexing~checked~count";
	
	public static final  String ACTIVITYINDEXLASTUPDATED = "activity~indexing~last~updated";
	
	public static final  String ACTIVITYINDEXSTATUS = "activity~indexing~status";
	
	public static final  String USERSESSION = "user~session";
	
	public static final  String AGGJSON = "aggregator_json";
	
	public static final  String ANONYMOUSSESSION = "anonymous~session";
	
	public static final  String ALLUSERSESSION = "all~user~session";
	
	public static final  String ACTIVESESSION = "active~session";
	
	public static final  String ACTIVECOLLECTIONPLAYS = "active~collection~plays";
	
	public static final  String ACTIVERESOURCEPLAYS = "active~resource~plays";
	
	public static final  String ACTIVECOLLECTIONRESOURCEPLAYS = "active~collection~resource~plays";
	
	public static final  String ACTIVEPLAYS = "active~content~plays";
	
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
	
	public static final  String INDEXINGVERSION = "index_version";
	
	public static final  String INDEXEVENTS = ".*collection.play.*|.*collection.resource.play.*|.*resource.play.*|.*reaction.create.*|.*item.rate.*|.*item.flag.*|.*item.create.*";
	
	public static final  String V2KAFKAMICROPRODUCER = "v2~kafka~microaggregator~producer";
	
	public static final  String V2KAFKALOGWRITERPRODUCER = "v2~kafka~logwritter~producer";
	
	public static final  String V2KAFKACONSUMER = "v2~kafka~consumer";
	
	public static final  String V2KAFKALOGWRITERCONSUMER = "v2~kafka~logwritter~consumer";
	
	public static final  String V2KAFKAMICROCONSUMER = "v2~kafka~microaggregator~consumer";
	
	public static final  String KAFKAIP =  "kafka_ip";
	
	public static final  String KAFKAPORT = "kafka_portno";
	
	public static final  String KAFKATOPIC = "kafka_topic";
	
	public static final  String KAFKAPRODUCERTYPE = "kafka_producertype";

	public static final int RECURSION_MAX_DEPTH = 20;
	
}
