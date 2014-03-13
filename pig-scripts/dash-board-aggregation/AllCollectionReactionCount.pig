REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET job.name 'agg_event_collection_reaction_count';

HOURLY_EVENTS = LOAD 'cassandra://$KEYSPACE/agg_event_resource_user_hour?split_size=32768' USING CassandraStorage() ;

STAGE_VALUES = FOREACH HOURLY_EVENTS GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_time_spent_ms.value as total_timespent_ms, organization_uid,event_value,resource_type,total_occurences.value as total_views;

DUPLICATE_STAGE_VALUES = DISTINCT STAGE_VALUES;

REACTION_EVENTS = FILTER DUPLICATE_STAGE_VALUES   BY (event_id.value == 217);

GROUPED_REACTION_VALUES = GROUP REACTION_EVENTS by (parent_gooru_oid,event_value,organization_uid);

FLATTENED_REACTION_VALUES = FOREACH GROUPED_REACTION_VALUES GENERATE StringConcat((group.parent_gooru_oid.value is null ? 'NA':group.parent_gooru_oid.value),'-',(group.organization_uid.value is null ? 'NA':group.organization_uid.value)) as KEY, FLATTEN(REACTION_EVENTS) ,SUM(REACTION_EVENTS.total_views) as reaction_count;

REACTIONS = LOAD 'cassandra://$KEYSPACE/reaction' USING CassandraStorage() ;

REACTION_IDS = FOREACH REACTIONS GENERATE key as reaction_names,reaction_id.value;

DUPLICATE_REACTION_IDS = DISTINCT REACTION_IDS;

GET_REACTION_VALUES =  JOIN FLATTENED_REACTION_VALUES BY REACTION_EVENTS::event_value.value,DUPLICATE_REACTION_IDS BY reaction_names;

AGG_HOURLY_REACTION_VALUES = FOREACH GET_REACTION_VALUES GENERATE StringConcat('C~',FLATTENED_REACTION_VALUES::REACTION_EVENTS::parent_gooru_oid.value) as key,TOTUPLE(StringConcat('C~RA~',DUPLICATE_REACTION_IDS::value),reaction_count) as reaction_count;

FILETR_VALID_REACTION_VALUES = FILTER AGG_HOURLY_REACTION_VALUES BY key is not null;

STORE FILETR_VALID_REACTION_VALUES INTO 'cassandra://$KEYSPACE/agg_event_collection_resource' USING CassandraStorage();
