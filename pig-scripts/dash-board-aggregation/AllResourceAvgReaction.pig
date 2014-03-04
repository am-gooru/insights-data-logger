REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET job.name 'agg_event_avg_resource_reaction';

HOURLY_EVENTS = LOAD 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage() ;

STAGE_VALUES = FOREACH HOURLY_EVENTS GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_time_spent_ms.value as total_timespent_ms, organization_uid,event_value,resource_type,total_occurences.value as total_views;

DUPLICATE_STAGE_VALUES = DISTINCT STAGE_VALUES;

REACTIONS = LOAD 'cassandra://$KEYSPACE/reaction' USING CassandraStorage() ;

REACTION_IDS = FOREACH REACTIONS GENERATE key as reaction_names,reaction_id.value; 

DUPLICATE_REACTION_IDS = DISTINCT REACTION_IDS;

REACTION_EVENTS = FILTER DUPLICATE_STAGE_VALUES   BY (event_id.value == 217);

GET_REACTION_VALUES =  JOIN REACTION_EVENTS BY event_value.value,DUPLICATE_REACTION_IDS BY reaction_names;

GROUPED_REACTION_VALUES = GROUP GET_REACTION_VALUES by (REACTION_EVENTS::parent_gooru_oid.value);

FLATTENED_REACTION_VALUES = FOREACH GROUPED_REACTION_VALUES GENERATE group as key_gooru_oid ,ROUND(AVG(GET_REACTION_VALUES.DUPLICATE_REACTION_IDS::value)) as reaction_avg_count;

AGG_HOURLY_REACTION_VALUES = FOREACH FLATTENED_REACTION_VALUES GENERATE StringConcat('C~',key_gooru_oid) as key,TOTUPLE('C~AVG~RA',reaction_avg_count) as reaction_avg_count;

FILETR_VALID_REACTION_VALUES = FILTER AGG_HOURLY_REACTION_VALUES BY key is not null;

STORE FILETR_VALID_REACTION_VALUES INTO 'cassandra://$KEYSPACE/agg_event_collection_resource' USING CassandraStorage();
