// You can run this command in cql client
// bin/cqlsh < <file path>/release_3.0_schema.cql 

//DROP TABLE students_class_activity;

//usage data will be available if user playing a collection or assessment from class

CREATE TABLE students_class_activity (
    class_uid text,
    user_uid text,
    collection_type text,
    course_uid text,
    unit_uid text,
    lesson_uid text,
    collection_uid text,
    attempt_status text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (class_uid, user_uid, collection_type, course_uid, unit_uid, lesson_uid, collection_uid)
) WITH CLUSTERING ORDER BY (user_uid ASC, collection_type ASC, course_uid ASC, unit_uid ASC, lesson_uid ASC, collection_uid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

    
// This columnfamily contains session ids if user playing a collection or assessment. 
// If user playing a collection/assessment from outside class, CUL id will be NA.
// NULL value columns can not filter.
CREATE TABLE user_sessions (
    user_uid text,
    collection_uid text,
    collection_type text,
    class_uid text,
    course_uid text,
    unit_uid text,
    lesson_uid text,
    session_id text,
    event_time bigint,
    event_type text,
    PRIMARY KEY (user_uid, collection_uid, collection_type, class_uid, course_uid, unit_uid, lesson_uid, session_id)
) WITH CLUSTERING ORDER BY (collection_uid ASC, collection_type ASC, class_uid ASC, course_uid ASC, unit_uid ASC, lesson_uid ASC, session_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


//This columnfamily contains usage data by session wise

CREATE TABLE user_session_activity (
    session_id text,
    gooru_oid text,
    collection_item_id text,
    answer_object text,
    answer_status text,
    attempts bigint,
    collection_type text,
    event_type text,
    question_type text,
    reaction bigint,
    resource_type text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (session_id, gooru_oid, collection_item_id)
) WITH CLUSTERING ORDER BY (gooru_oid ASC, collection_item_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


//This columnfamily contains usage by standards and learning target wise
//This will captures data only if student playing a collection or assessment from class

CREATE TABLE content_taxonomy_activity (
    user_uid text,
    resource_type text,
    subject_id text,
    course_id text,
    domain_id text,
    standards_id text,
    learning_targets_id text,
    gooru_oid text,
    question_type text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (user_uid, resource_type, subject_id, course_id, domain_id, standards_id, learning_targets_id, gooru_oid)
) WITH CLUSTERING ORDER BY (resource_type ASC, subject_id ASC, course_id ASC, domain_id ASC, standards_id ASC, learning_targets_id ASC, gooru_oid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


CREATE TABLE content_class_taxonomy_activity (
    class_uid text,
    user_uid text,
    resource_type text,
    subject_id text,
    course_id text,
    domain_id text,
    standards_id text,
    learning_targets_id text,
    gooru_oid text,
    question_type text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (class_uid, user_uid, resource_type, subject_id, course_id, domain_id, standards_id, learning_targets_id, gooru_oid)
) WITH CLUSTERING ORDER BY (user_uid ASC, resource_type ASC, subject_id ASC, course_id ASC, domain_id ASC, standards_id ASC, learning_targets_id ASC, gooru_oid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

	
//This columnfamily contains data where student is working actively

CREATE TABLE student_location (
    class_uid text,
    user_uid text,
    collection_type text,
    collection_uid text,
    course_uid text,
    lesson_uid text,
    resource_uid text,
    session_time bigint,
    unit_uid text,
    PRIMARY KEY (class_uid, user_uid)
) WITH CLUSTERING ORDER BY (user_uid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


//This columnfamily contains no.of active and in active users count
    
CREATE TABLE class_activity_peer_counts (
    row_key text,
    leaf_gooru_oid text,
    collection_type text,
    active_peer_count counter,
    left_peer_count counter,
    PRIMARY KEY (row_key, leaf_gooru_oid, collection_type)
) WITH CLUSTERING ORDER BY (leaf_gooru_oid ASC, collection_type ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
  
  CREATE TABLE class_activity_peer_detail (
    row_key text,
    leaf_gooru_oid text,
    collection_type text,
    active_peers set<text>,
    left_peers set<text>,
    PRIMARY KEY (row_key, leaf_gooru_oid, collection_type)
) WITH CLUSTERING ORDER BY (leaf_gooru_oid ASC, collection_type ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
        
CREATE TABLE class_activity_datacube (
    row_key text,
    collection_type text,
    user_uid text,
    leaf_node text,
    completed_count bigint,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (row_key, collection_type, user_uid, leaf_node)
) WITH CLUSTERING ORDER BY (collection_type ASC, user_uid ASC, leaf_node ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


CREATE TABLE taxonomy_activity_datacube (
    row_key text,
    leaf_node text,
    question_timespent bigint,
    question_views bigint,
    questions_attempted bigint,
    resource_timespent bigint,
    resource_views bigint,
    resources_viewed bigint,
    score bigint,
    PRIMARY KEY (row_key, leaf_node)
) WITH CLUSTERING ORDER BY (leaf_node ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE user_question_grade (
    teacher_id text,
    user_id text,
    session_id text,
    question_id text,
    score bigint,
    PRIMARY KEY (teacher_id, user_id, session_id, question_id)
) WITH CLUSTERING ORDER BY (user_id ASC, session_id ASC, question_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128                                                                                                                                                                
    AND read_repair_chance = 0.0                                                                                                                                                                
    AND speculative_retry = '99.0PERCENTILE';
    
    
 CREATE TABLE taxonomy_nodes (
    row_key text PRIMARY KEY,
    course_id text,
    domain_id text,
    learning_targets_id text,
    standards_id text,
    subject_id text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
   
CREATE TABLE taxonomy_child_count (
    row_key text PRIMARY KEY,
    child_count bigint,
    question_count bigint,
    type_name text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
    
CREATE TABLE class_content_count (
    row_key text PRIMARY KEY,
    assessment_count bigint,
    collection_count bigint,
    type_name text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
    
   