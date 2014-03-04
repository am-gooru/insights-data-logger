STAGE = LOAD 'cassandra://event_logger_insights/stging_event_resource_user' USING CassandraStorage() ;

STAGE_VALUES = FOREACH STAGE GENERATE key, hour_id,date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_timespent_ms,organization_uid,event_value,resource_type,app_key,app_id;

GROUPED_VALUES = GROUP STAGE_VALUES by (hour_id,date_id,event_id,gooru_oid,organization_uid,event_value,app_id);

FLATTENED_VALUES = FOREACH GROUPED_VALUES GENERATE StringConcat((group.hour_id.value),'-',(group.date_id.value),'-',(group.event_id.value),'-' ,((group.gooru_oid.value) is null ? 'NA':group.gooru_oid.value),'-',((group.organization_uid.value)is null ? 'NA':group.organization_uid.value),'-',((group.event_value.value) is null ? 'NA':group.event_value.value),'-',((group.app_id.value) is null ? 'NA':group.app_id.value)) as KEY, FLATTEN(STAGE_VALUES) , COUNT($1) as total_occurences;

AGG_HOURLY_VALUES = FOREACH FLATTENED_VALUES GENERATE KEY as key,TOTUPLE('hour_id',hour_id.value) as hour_id,TOTUPLE('date_id',date_id.value) as date_id ,TOTUPLE('event_id',event_id.value) as event_id,TOTUPLE('gooru_oid',gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',user_uid.value) as user_uid,TOTUPLE('event_value',event_value.value) as event_value,TOTUPLE('parent_gooru_oid',parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',total_timespent_ms.value) as total_timespent_ms,TOTUPLE('app_id', app_id.value) as app_id,TOTUPLE('app_key', app_key.value) as app_key,TOTUPLE('total_occurences', total_occurences) as total_occurences,TOTUPLE('organization_uid', organization_uid) as organization_uid;

FILETR_VALID_HOUR_VALUES = FILTER AGG_HOURLY_VALUES BY key is not null;

STORE FILETR_VALID_HOUR_VALUES INTO 'cassandra://event_logger_insights/agg_event_resource_world_hour' USING CassandraStorage();

AGG_HOURLY_VALUES_DB = FOREACH FLATTENED_VALUES GENERATE hour_id.value as hour_id,date_id.value as date_id ,event_id.value as event_id,gooru_oid.value as gooru_oid,user_uid.value as user_uid,event_value.value as event_value,parent_gooru_oid.value as parent_gooru_oid,total_timespent_ms.value as total_timespent_ms, total_occurences as total_occurences,organization_uid.value as organization_uid,app_id.value as app_id,app_key.value as app_key;

STORE AGG_HOURLY_VALUES_DB into 'agg_event_resource_world_hour' using  org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://dadb.goorulearning.org/gooru_insights_qa','gl_qa_user','GtXfmGeEq@W3VwAU','insert into agg_event_resource_world_hour (time_id,date_id,event_id,gooru_oid,user_uid,event_value,parent_gooru_oid,total_time_spent_ms,total_occurences,organization_uid,app_id,app_key) values (?,?,?,?,?,?,?,?,?,?,?,?)');

HOUR_VAL = LOAD 'cassandra://event_logger_insights/agg_event_resource_world_hour' USING CassandraStorage() ;

DIM_DATE = LOAD 'cassandra://event_logger_insights/dim_date' USING CassandraStorage() ;

CURR_DATE = FILTER DIM_DATE BY (date.value == '$DATE 00:00:00+0000');

ALL_DATE_IDS = FOREACH CURR_DATE GENERATE date.value as current_day,date_id.value as date_id,(date_id.value - 1 ) as last_date_id,week_date_id.value as week_date_id,month_date_id.value as month_date_id,year_date_id.value as year_date_id;

DAY_DATE_ID = FOREACH DIM_DATE GENERATE date_id as keyIds,week_date_id; 

FILTER_PROCESSING_DAY = FOREACH HOUR_VAL GENERATE  date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,organization_uid,event_value,total_occurences.value as total_views,total_time_spent_ms.value as total_time_spent ,total_likes.value as likes,total_adds.value as adds ,total_score.value as scores,app_id as app_id,app_key as app_key;

HOURLY_VALUES = FILTER FILTER_PROCESSING_DAY  BY (date_id.value == ALL_DATE_IDS.date_id) OR (date_id.value == ALL_DATE_IDS.last_date_id);

GROUPED_HOURLY_VALUES = GROUP HOURLY_VALUES by (date_id,event_id,gooru_oid,organization_uid,event_value,app_id);

FLATTENED_HOURLY_VALUES = FOREACH GROUPED_HOURLY_VALUES GENERATE StringConcat((group.date_id.value),'-',(group.event_id.value),'-',((group.gooru_oid.value) is null ? 'NA':group.gooru_oid.value),'-',((group.organization_uid.value)is null ? 'NA':group.organization_uid.value),'-',((group.event_value.value) is null ? 'NA':group.event_value.value),'-',((group.app_id.value) is null ? 'NA':group.app_id.value)) as KEY, FLATTEN(HOURLY_VALUES),SUM(HOURLY_VALUES.total_views) as total_occurences,SUM(HOURLY_VALUES.total_time_spent) as total_time_spent_ms,SUM(HOURLY_VALUES.adds) as total_adds,SUM(HOURLY_VALUES.likes) as total_likes,SUM(HOURLY_VALUES.scores) as total_score;

AGG_DAY_VALUES =  JOIN FLATTENED_HOURLY_VALUES BY date_id.value,DAY_DATE_ID BY keyIds.value;

DAY_RESULT = FOREACH AGG_DAY_VALUES GENERATE FLATTENED_HOURLY_VALUES::KEY as key,TOTUPLE('date_id',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::date_id.value) as date_id ,TOTUPLE('event_id',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::event_id.value) as event_id,TOTUPLE('gooru_oid',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::user_uid.value) as user_uid,TOTUPLE('event_value',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::event_value.value) as event_value,TOTUPLE('parent_gooru_oid',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',FLATTENED_HOURLY_VALUES::total_time_spent_ms) as total_time_spent_ms,TOTUPLE('total_occurences',FLATTENED_HOURLY_VALUES::total_occurences) as total_occurences,TOTUPLE('total_adds',FLATTENED_HOURLY_VALUES::total_adds) as total_adds,TOTUPLE('total_likes',FLATTENED_HOURLY_VALUES::total_likes) as total_likes,TOTUPLE('total_score',FLATTENED_HOURLY_VALUES::total_score) as total_score,TOTUPLE('week_date_id',DAY_DATE_ID::week_date_id.value) as week_date_id,TOTUPLE('app_id',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::app_id.value) as app_id,TOTUPLE('app_key',FLATTENED_HOURLY_VALUES::HOURLY_VALUES::app_key.value) as app_key;

FILETR_VALID_DAILY_VALUES = FILTER DAY_RESULT BY key is not null;

STORE FILETR_VALID_DAILY_VALUES INTO 'cassandra://event_logger_insights/agg_event_resource_world_day' USING CassandraStorage();

DAY_RESULT_DB = FOREACH AGG_DAY_VALUES GENERATE FLATTENED_HOURLY_VALUES::HOURLY_VALUES::date_id.value as date_id ,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::event_id.value as event_id,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::gooru_oid.value as gooru_oid,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::user_uid.value as user_uid,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::event_value.value as event_value,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::parent_gooru_oid.value as parent_gooru_oid,FLATTENED_HOURLY_VALUES::total_time_spent_ms as total_time_spent_ms,FLATTENED_HOURLY_VALUES::total_occurences as total_occurences,DAY_DATE_ID::week_date_id.value as week_date_id,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::app_id.value as app_id,FLATTENED_HOURLY_VALUES::HOURLY_VALUES::app_key.value as app_key;

STORE DAY_RESULT_DB into 'agg_event_resource_world_day' using  org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://dadb.goorulearning.org/gooru_insights_qa','gl_qa_user','GtXfmGeEq@W3VwAU','insert into agg_event_resource_world_day (date_id,event_id,gooru_oid,user_uid,event_value,parent_gooru_oid,total_time_spent_ms,total_occurences,week_date_id,app_id,app_key) values (?,?,?,?,?,?,?,?,?,?,?)');

DAY_VAL = LOAD 'cassandra://event_logger_insights/agg_event_resource_world_day' USING CassandraStorage() ;

WEEK_DATE_ID = FOREACH DIM_DATE GENERATE date_id as weekKeyIds,month_date_id; 

FILTER_PROCESSING_WEEK = FOREACH DAY_VAL GENERATE  week_date_id as date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,organization_uid,event_value,total_occurences.value as total_views,total_time_spent_ms.value as total_time_spent ,total_likes.value as likes,total_adds.value as adds ,total_score.value as scores,app_id,app_key;

DAY_VALUES = FILTER FILTER_PROCESSING_WEEK  BY (date_id.value == ALL_DATE_IDS.week_date_id);

GROUPED_DAY_VALUES = GROUP DAY_VALUES by (date_id,event_id,gooru_oid,organization_uid,event_value,app_id);

FLATTENED_DAY_VALUES = FOREACH GROUPED_DAY_VALUES GENERATE StringConcat((group.date_id.value),'-',(group.event_id.value),'-',((group.gooru_oid.value) is null ? 'NA':group.gooru_oid.value),'-',((group.organization_uid.value)is null ? 'NA':group.organization_uid.value),'-',((group.event_value.value) is null ? 'NA':group.event_value.value),'-',((group.app_id.value) is null ? 'GLP':group.app_id.value)) as KEY, FLATTEN(DAY_VALUES),SUM(DAY_VALUES.total_views) as total_occurences,SUM(DAY_VALUES.total_time_spent) as total_time_spent_ms,SUM(DAY_VALUES.adds) as total_adds,SUM(DAY_VALUES.likes) as total_likes,SUM(DAY_VALUES.scores) as total_score;

AGG_WEEK_VALUES =  JOIN FLATTENED_DAY_VALUES BY date_id.value,WEEK_DATE_ID BY weekKeyIds.value;

WEEK_RESULT = FOREACH AGG_WEEK_VALUES GENERATE FLATTENED_DAY_VALUES::KEY as key,TOTUPLE('date_id',FLATTENED_DAY_VALUES::DAY_VALUES::date_id.value) as date_id ,TOTUPLE('event_id',FLATTENED_DAY_VALUES::DAY_VALUES::event_id.value) as event_id,TOTUPLE('gooru_oid',FLATTENED_DAY_VALUES::DAY_VALUES::gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',FLATTENED_DAY_VALUES::DAY_VALUES::user_uid.value) as user_uid,TOTUPLE('event_value',FLATTENED_DAY_VALUES::DAY_VALUES::event_value.value) as event_value,TOTUPLE('parent_gooru_oid',FLATTENED_DAY_VALUES::DAY_VALUES::parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',FLATTENED_DAY_VALUES::total_time_spent_ms) as total_time_spent_ms,TOTUPLE('total_occurences',FLATTENED_DAY_VALUES::total_occurences) as total_occurences,TOTUPLE('total_adds',FLATTENED_DAY_VALUES::total_adds) as total_adds,TOTUPLE('total_likes',FLATTENED_DAY_VALUES::total_likes) as total_likes,TOTUPLE('total_score',FLATTENED_DAY_VALUES::total_score) as total_score,TOTUPLE('month_date_id',WEEK_DATE_ID::month_date_id.value) as month_date_id,TOTUPLE('app_id',FLATTENED_DAY_VALUES::DAY_VALUES::app_id.value) as app_id,TOTUPLE('app_key',FLATTENED_DAY_VALUES::DAY_VALUES::app_key.value) as app_key;

FILETR_VALID_WEEK_VALUES = FILTER WEEK_RESULT BY key is not null;

STORE FILETR_VALID_WEEK_VALUES INTO 'cassandra://event_logger_insights/agg_event_resource_world_week' USING CassandraStorage();

WEEK_RESULT_DB = FOREACH AGG_WEEK_VALUES GENERATE FLATTENED_DAY_VALUES::DAY_VALUES::date_id.value as date_id ,FLATTENED_DAY_VALUES::DAY_VALUES::event_id.value as event_id,FLATTENED_DAY_VALUES::DAY_VALUES::gooru_oid.value as gooru_oid,FLATTENED_DAY_VALUES::DAY_VALUES::user_uid.value as user_uid,FLATTENED_DAY_VALUES::DAY_VALUES::event_value.value as event_value,FLATTENED_DAY_VALUES::DAY_VALUES::parent_gooru_oid.value as parent_gooru_oid,FLATTENED_DAY_VALUES::total_time_spent_ms as total_time_spent_ms,FLATTENED_DAY_VALUES::total_occurences as total_occurences,WEEK_DATE_ID::month_date_id.value as month_date_id,FLATTENED_DAY_VALUES::DAY_VALUES::app_id.value as app_id,FLATTENED_DAY_VALUES::DAY_VALUES::app_key.value as app_key;

STORE WEEK_RESULT_DB into 'agg_event_resource_world_week' using  org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://dadb.goorulearning.org/gooru_insights_qa','gl_qa_user','GtXfmGeEq@W3VwAU','insert into agg_event_resource_world_week (date_id,event_id,gooru_oid,user_uid,event_value,parent_gooru_oid,total_time_spent_ms,total_occurences,month_date_id,app_id,app_key) values (?,?,?,?,?,?,?,?,?,?,?)');

WEEK_VAL = LOAD 'cassandra://event_logger_insights/agg_event_resource_world_week' USING CassandraStorage() ;

MONTH_DATE_ID = FOREACH DIM_DATE GENERATE date_id as monthKeyIds,year_date_id; 

FILTER_PROCESSING_MONTH = FOREACH WEEK_VAL GENERATE  month_date_id as date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,organization_uid,event_value,total_occurences.value as total_views,total_time_spent_ms.value as total_time_spent ,total_likes.value as likes,total_adds.value as adds ,total_score.value as scores,app_id,app_key;

WEEKLY_VALUES = FILTER FILTER_PROCESSING_MONTH  BY (date_id.value == ALL_DATE_IDS.month_date_id);

GROUPED_WEEKLY_VALUES = GROUP WEEKLY_VALUES by (date_id,event_id,gooru_oid,organization_uid,event_value,app_id);

FLATTENED_WEEKLY_VALUES = FOREACH GROUPED_WEEKLY_VALUES GENERATE StringConcat((group.date_id.value),'-',(group.event_id.value),'-',((group.gooru_oid.value) is null ? 'NA':group.gooru_oid.value),'-',((group.organization_uid.value) is null ? 'NA':group.organization_uid.value),'-',((group.event_value.value) is null ? 'NA':group.event_value.value),'-',((group.app_id.value) is null ? 'GLP':group.app_id.value)) as KEY, FLATTEN(WEEKLY_VALUES),SUM(WEEKLY_VALUES.total_views) as total_occurences,SUM(WEEKLY_VALUES.total_time_spent) as total_time_spent_ms,SUM(WEEKLY_VALUES.adds) as total_adds,SUM(WEEKLY_VALUES.likes) as total_likes,SUM(WEEKLY_VALUES.scores) as total_score;

AGG_MONTHLY_VALUES =  JOIN FLATTENED_WEEKLY_VALUES BY date_id.value,MONTH_DATE_ID BY monthKeyIds.value;

MONTH_RESULT = FOREACH AGG_MONTHLY_VALUES GENERATE FLATTENED_WEEKLY_VALUES::KEY as key,TOTUPLE('date_id',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::date_id.value) as date_id ,TOTUPLE('event_id',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::event_id.value) as event_id,TOTUPLE('gooru_oid',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::user_uid.value) as user_uid,TOTUPLE('event_value',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::event_value.value) as event_value,TOTUPLE('parent_gooru_oid',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',FLATTENED_WEEKLY_VALUES::total_time_spent_ms) as total_time_spent_ms,TOTUPLE('total_occurences',FLATTENED_WEEKLY_VALUES::total_occurences) as total_occurences,TOTUPLE('total_adds',FLATTENED_WEEKLY_VALUES::total_adds) as total_adds,TOTUPLE('total_likes',FLATTENED_WEEKLY_VALUES::total_likes) as total_likes,TOTUPLE('total_score',FLATTENED_WEEKLY_VALUES::total_score) as total_score,TOTUPLE('year_date_id',MONTH_DATE_ID::year_date_id.value) as year_date_id,TOTUPLE('app_id',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::app_id.value) as app_id,TOTUPLE('app_key',FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::app_key.value) as app_key;

FILETR_VALID_MONTH_VALUES = FILTER MONTH_RESULT BY key is not null;

STORE FILETR_VALID_MONTH_VALUES INTO 'cassandra://event_logger_insights/agg_event_resource_world_month' USING CassandraStorage();

MONTH_RESULT_DB = FOREACH AGG_MONTHLY_VALUES GENERATE FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::date_id.value as date_id ,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::event_id.value as event_id,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::gooru_oid.value as gooru_oid,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::user_uid.value as user_uid,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::event_value.value as event_value,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::parent_gooru_oid.value as parent_gooru_oid, FLATTENED_WEEKLY_VALUES::total_time_spent_ms as total_time_spent_ms,FLATTENED_WEEKLY_VALUES::total_occurences as total_occurences,MONTH_DATE_ID::year_date_id.value as year_date_id,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::app_id.value as app_id,FLATTENED_WEEKLY_VALUES::WEEKLY_VALUES::app_key.value as app_key;

STORE MONTH_RESULT_DB into 'agg_event_resource_world_month' using  org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://dadb.goorulearning.org/gooru_insights_qa','gl_qa_user','GtXfmGeEq@W3VwAU','insert into agg_event_resource_world_month (date_id,event_id,gooru_oid,user_uid,event_value,parent_gooru_oid,total_time_spent_ms,total_occurences,year_date_id,app_id,app_key) values (?,?,?,?,?,?,?,?,?,?,?)');

MONTH_VAL = LOAD 'cassandra://event_logger_insights/agg_event_resource_world_month' USING CassandraStorage() ;

YEAR_DATE_ID = FOREACH DIM_DATE GENERATE date_id as yearKeyIds,year_date_id; 

FILTER_PROCESSING_YEAR = FOREACH MONTH_VAL GENERATE  year_date_id as date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,organization_uid,event_value,total_occurences.value as total_views,total_time_spent_ms.value as total_time_spent ,total_likes.value as likes,total_adds.value as adds ,total_score.value as scores,app_id,app_key;

MONTHLY_VALUES = FILTER FILTER_PROCESSING_YEAR  BY (date_id.value == ALL_DATE_IDS.year_date_id);

GROUPED_MONTHLY_VALUES = GROUP MONTHLY_VALUES by (date_id,event_id,gooru_oid,organization_uid,event_value,app_id);

FLATTENED_MONTHLY_VALUES = FOREACH GROUPED_MONTHLY_VALUES GENERATE StringConcat((group.date_id.value),'-',(group.event_id.value),'-',((group.gooru_oid.value) is null ? 'NA':group.gooru_oid.value),'-',((group.organization_uid.value)is null ? 'NA':group.organization_uid.value),'-',((group.event_value.value) is null ? 'NA':group.event_value.value),'-',((group.app_id.value) is null ? 'GLP':group.app_id.value)) as KEY, FLATTEN(MONTHLY_VALUES),SUM(MONTHLY_VALUES.total_views) as total_occurences,SUM(MONTHLY_VALUES.total_time_spent) as total_time_spent_ms,SUM(MONTHLY_VALUES.adds) as total_adds,SUM(MONTHLY_VALUES.likes) as total_likes,SUM(MONTHLY_VALUES.scores) as total_score;

AGG_YEARLY_VALUES =  JOIN FLATTENED_MONTHLY_VALUES BY date_id.value,YEAR_DATE_ID BY yearKeyIds.value;

YEAR_RESULT = FOREACH AGG_YEARLY_VALUES GENERATE FLATTENED_MONTHLY_VALUES::KEY as key,TOTUPLE('date_id',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::date_id.value) as date_id ,TOTUPLE('event_id',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::event_id.value) as event_id,TOTUPLE('gooru_oid',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::user_uid.value) as user_uid,TOTUPLE('event_value',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::event_value.value) as event_value,TOTUPLE('parent_gooru_oid',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',FLATTENED_MONTHLY_VALUES::total_time_spent_ms) as total_time_spent_ms,TOTUPLE('total_occurences',FLATTENED_MONTHLY_VALUES::total_occurences) as total_occurences,TOTUPLE('total_adds',FLATTENED_MONTHLY_VALUES::total_adds) as total_adds,TOTUPLE('total_likes',FLATTENED_MONTHLY_VALUES::total_likes) as total_likes,TOTUPLE('total_score',FLATTENED_MONTHLY_VALUES::total_score) as total_score,TOTUPLE('app_id',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::app_id.value) as app_id,TOTUPLE('app_key',FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::app_key.value) as app_key;

FILETR_VALID_YEAR_VALUES = FILTER YEAR_RESULT BY key is not null;

STORE FILETR_VALID_YEAR_VALUES INTO 'cassandra://event_logger_insights/agg_event_resource_world_year' USING CassandraStorage();

YEAR_RESULT_DB = FOREACH AGG_YEARLY_VALUES GENERATE FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::date_id.value as date_id ,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::event_id.value as event_id,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::gooru_oid.value as gooru_oid,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::user_uid.value as user_uid,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::event_value.value as event_value,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::parent_gooru_oid.value as parent_gooru_oid,FLATTENED_MONTHLY_VALUES::total_time_spent_ms as total_time_spent_ms,FLATTENED_MONTHLY_VALUES::total_occurences as total_occurences,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::app_id.value as app_id,FLATTENED_MONTHLY_VALUES::MONTHLY_VALUES::app_key.value as app_key;

STORE YEAR_RESULT_DB into 'agg_event_resource_world_year' using  org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://dadb.goorulearning.org/gooru_insights_qa','gl_qa_user','GtXfmGeEq@W3VwAU','insert into agg_event_resource_world_year (date_id,event_id,gooru_oid,user_uid,event_value,parent_gooru_oid,total_time_spent_ms,total_occurences,app_id,app_key) values (?,?,?,?,?,?,?,?,?,?)');

YEAR_VAL = LOAD 'cassandra://event_logger_insights/agg_event_resource_world_year' USING CassandraStorage() ;

YEARLY_VALUES = FOREACH YEAR_VAL GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,organization_uid,event_value,total_occurences.value as total_views,total_time_spent_ms.value as total_time_spent ,total_likes.value as likes,total_adds.value as adds ,total_score.value as scores,app_id,app_key;

GROUPED_YEARLY_VALUES = GROUP YEARLY_VALUES by (event_id,gooru_oid,organization_uid,event_value,app_id);

FLATTENED_YEARLY_VALUES = FOREACH GROUPED_YEARLY_VALUES GENERATE StringConcat((group.event_id.value),'-' ,((group.gooru_oid.value) is null ? 'NA':group.gooru_oid.value),'-',((group.organization_uid.value)is null ? 'NA':group.organization_uid.value),'-',((group.event_value.value) is null ? 'NA':group.event_value.value),'-',((group.app_id.value) is null ? 'GLP':group.app_id.value)) as KEY, FLATTEN(YEARLY_VALUES),SUM(YEARLY_VALUES.total_views) as total_occurences,SUM(YEARLY_VALUES.total_time_spent) as total_time_spent_ms,SUM(YEARLY_VALUES.adds) as total_adds,SUM(YEARLY_VALUES.likes) as total_likes,SUM(YEARLY_VALUES.scores) as total_score;

ALL_RESULT = FOREACH FLATTENED_YEARLY_VALUES GENERATE KEY as key,TOTUPLE('event_id',YEARLY_VALUES::event_id.value) as event_id,TOTUPLE('gooru_oid',YEARLY_VALUES::gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',YEARLY_VALUES::user_uid.value) as user_uid,TOTUPLE('event_value',YEARLY_VALUES::event_value.value) as event_value,TOTUPLE('parent_gooru_oid',YEARLY_VALUES::parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',total_time_spent_ms) as total_time_spent_ms,TOTUPLE('total_occurences',total_occurences) as total_occurences,TOTUPLE('total_adds',total_adds) as total_adds,TOTUPLE('total_likes',total_likes) as total_likes,TOTUPLE('total_score',total_score) as total_score,TOTUPLE('app_id',YEARLY_VALUES::app_id.value) as app_id,TOTUPLE('app_key',YEARLY_VALUES::app_key.value) as app_key;

FILETR_VALID_ALL_VALUES = FILTER ALL_RESULT BY key is not null;

STORE FILETR_VALID_ALL_VALUES INTO 'cassandra://event_logger_insights/agg_event_resource_world_all' USING CassandraStorage();

ALL_RESULT_DB = FOREACH FLATTENED_YEARLY_VALUES GENERATE YEARLY_VALUES::event_id.value as event_id,YEARLY_VALUES::gooru_oid.value as gooru_oid,YEARLY_VALUES::user_uid.value as user_uid,YEARLY_VALUES::event_value.value as event_value,YEARLY_VALUES::parent_gooru_oid.value as parent_gooru_oid,total_time_spent_ms as total_time_spent_ms,total_occurences as total_occurences,YEARLY_VALUES::app_id.value as app_id,YEARLY_VALUES::app_key.value as app_key;

STORE ALL_RESULT_DB into 'agg_event_resource_world_all' using  org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver','jdbc:mysql://dadb.goorulearning.org/gooru_insights_qa','gl_qa_user','GtXfmGeEq@W3VwAU','insert into agg_event_resource_world_all (event_id,gooru_oid,user_uid,event_value,parent_gooru_oid,total_time_spent_ms,total_occurences,app_id,app_key) values (?,?,?,?,?,?,?,?,?)');


