CREATE TABLE taxonomy_code (
    key text PRIMARY KEY,
    code text,
    code_id text,
    depth bigint,
    label text,
    root_node_id text,
    type_id bigint
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = '99.0PERCENTILE';



CREATE TABLE dim_events_list (
    key text PRIMARY KEY,
    api_key text,
    display_name text,
    display_order bigint,
    event_id text,
    event_name text,
    property text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_sstable_size': '52428800', 'max_threshold': '32', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'min_threshold': '4'}
    AND compression = {'chunk_length_kb': '64', 'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX api_key_list_idx ON dim_events_list (api_key);
CREATE INDEX dim_events_event_list_id_idx ON dim_events_list (event_id);
CREATE INDEX dim_event_name_list_idx ON dim_events_list (event_name);



CREATE TABLE real_time_aggregator (
    key text PRIMARY KEY,
    score bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE resource_format (
    key text PRIMARY KEY,
    id bigint
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE event_fields (
    key text PRIMARY KEY,
    be_column text,
    description text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';


CREATE TABLE api_key (
    key text PRIMARY KEY,
    active_flag int,
    api_key text,
    app_name text,
    app_url text,
    comment text,
    description text,
    last_modified_date timestamp,
    organization_uid text,
    status text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX active_flag ON api_key (active_flag);
CREATE INDEX api_key ON api_key (api_key);
CREATE INDEX app_url ON api_key (app_url);
CREATE INDEX last_modified_date ON api_key (last_modified_date);
CREATE INDEX api_key_status ON api_key (status);


CREATE TABLE job_indexing_queue (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = '99.0PERCENTILE';



CREATE TABLE dim_resource (
    key text PRIMARY KEY,
    app_id text,
    app_key text,
    attribution text,
    category text,
    content_id bigint,
    copied_resource_id text,
    course_content_id bigint,
    created_on text,
    creator_uid text,
    description text,
    gooru_oid text,
    grade text,
    instructional text,
    instructional_id bigint,
    is_oer boolean,
    last_accessed bigint,
    last_accessed_user text,
    last_modified text,
    last_updated_user_uid text,
    length text,
    license_name text,
    network text,
    organization_uid text,
    record_source text,
    resource_format text,
    resource_format_id bigint,
    resource_type bigint,
    sharing text,
    thumbnail text,
    title text,
    type_name text,
    url text,
    user_uid text,
    views_count bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX app_id_index ON dim_resource (app_id);
CREATE INDEX app_key_index ON dim_resource (app_key);
CREATE INDEX category_index ON dim_resource (category);
CREATE INDEX content_id_index ON dim_resource (content_id);
CREATE INDEX gooru_oid_index ON dim_resource (gooru_oid);
CREATE INDEX user_uid_index ON dim_resource (user_uid);


CREATE TABLE resource (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = '99.0PERCENTILE';


CREATE TABLE assessment_answer (
    key text PRIMARY KEY,
    answer_explanation text,
    answer_group_code text,
    answer_hint text,
    answer_id bigint,
    answer_text text,
    collection_gooru_oid text,
    "endTime" timestamp,
    is_correct int,
    matching_answer_id int,
    question_gooru_oid text,
    question_id bigint,
    question_type text,
    result boolean,
    sequence int,
    type_name text,
    unit text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX collection_gooru_oid ON assessment_answer (collection_gooru_oid);
CREATE INDEX is_correct ON assessment_answer (is_correct);
CREATE INDEX question_gooru_oid ON assessment_answer (question_gooru_oid);
CREATE INDEX question_id ON assessment_answer (question_id);
CREATE INDEX sequence ON assessment_answer (sequence);


CREATE TABLE custom_fields_group (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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
    AND speculative_retry = 'NONE';




CREATE TABLE activity_stream (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE dim_content_classification (
    key text PRIMARY KEY,
    code_id bigint,
    content_id bigint,
    depth int,
    gooru_oid text,
    label text,
    root_node_id bigint
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX code_id_ccl ON dim_content_classification (code_id);
CREATE INDEX content_id_ccl ON dim_content_classification (content_id);
CREATE INDEX gooru_oid_ccl ON dim_content_classification (gooru_oid);



CREATE TABLE event_detail (
    key text PRIMARY KEY,
    api_key text,
    app_oid text,
    app_uid text,
    city text,
    content_gooru_oid text,
    country text,
    end_time bigint,
    event_name text,
    event_source text,
    event_value text,
    fields text,
    gooru_uid text,
    parent_gooru_oid text,
    session_token text,
    start_time bigint,
    state text,
    time_spent_in_millis bigint,
    user_agent text,
    user_id text,
    user_ip text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX apiKey ON event_detail (api_key);
CREATE INDEX city ON event_detail (city);
CREATE INDEX content_gooru_oid ON event_detail (content_gooru_oid);
CREATE INDEX country ON event_detail (country);
CREATE INDEX end_time ON event_detail (end_time);
CREATE INDEX event_name ON event_detail (event_name);
CREATE INDEX event_source ON event_detail (event_source);
CREATE INDEX gooruUid ON event_detail (gooru_uid);
CREATE INDEX parent_gooruoid ON event_detail (parent_gooru_oid);
CREATE INDEX session_token ON event_detail (session_token);
CREATE INDEX event_detail_start_time_idx ON event_detail (start_time);
CREATE INDEX state ON event_detail (state);
CREATE INDEX user_ip ON event_detail (user_ip);



CREATE TABLE category (
    key text PRIMARY KEY,
    id bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE dim_events (
    key text PRIMARY KEY,
    api_key text,
    display_name text,
    display_order bigint,
    event_id text,
    event_name text,
    property text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"100"}'
    AND comment = ''
    AND compaction = {'min_sstable_size': '52428800', 'max_threshold': '32', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'min_threshold': '4'}
    AND compression = {'chunk_length_kb': '64', 'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX api_key_idx ON dim_events (api_key);
CREATE INDEX dim_events_event_id_idx ON dim_events (event_id);
CREATE INDEX dim_event_name_idx ON dim_events (event_name);



CREATE TABLE app_api_key (
    key text PRIMARY KEY,
    "appName" text,
    "appOid" text,
    "endPoint" text,
    "pushIntervalMs" text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX appOid ON app_api_key ("appOid");


CREATE TABLE party_info (
    key text PRIMARY KEY,
    parent_uuid text,
    party_id bigint,
    party_name text,
    party_type text
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
CREATE INDEX party_info_parent_uuid_idx ON party_info (parent_uuid);
CREATE INDEX party_info_party_type_idx ON party_info (party_type);



CREATE TABLE session_activity (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE collection_item_assoc (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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



CREATE TABLE event_timeline (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE job_tracker (
    key text PRIMARY KEY,
    column_list text,
    end_time text,
    fields text,
    index_api text,
    index_datasource text,
    index_flag int,
    index_limit bigint,
    index_on text,
    indexed_count bigint,
    job_interval bigint,
    job_name text,
    job_path text,
    key_column text,
    list_indexes text,
    max_allowed_content text,
    max_count bigint,
    max_jobs text,
    modified_by text,
    modified_on text,
    monitor_job int,
    offset bigint,
    owner text,
    process_type text,
    query text,
    request_method text,
    running_jobs text,
    running_status int,
    running_type text,
    server_name text,
    session_token text,
    start_time text,
    start_value text,
    status int,
    target_columnfamily text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX monitor_job ON job_tracker (monitor_job);
CREATE INDEX running_status ON job_tracker (running_status);



CREATE TABLE class_activity (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE anonymized_user_data (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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



CREATE TABLE collection_item (
    key text PRIMARY KEY,
    associated_by_uid text,
    association_date timestamp,
    collection_content_id bigint,
    collection_gooru_oid text,
    collection_item text,
    collection_item_id text,
    "deleteStatus" bigint,
    deleted int,
    deleted_1 int,
    estimated_time text,
    is_required int,
    item_sequence int,
    item_type text,
    minimum_score text,
    narration text,
    narration_type text,
    planned_end_date timestamp,
    question_type text,
    resource_content_id bigint,
    resource_gooru_oid text,
    start text,
    stop text,
    version int
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX collection_item_collection_content_id_idx ON collection_item (collection_content_id);
CREATE INDEX collection_item_collection_gooru_oid_idx ON collection_item (collection_gooru_oid);
CREATE INDEX collection_item_collection_item_id_idx_1 ON collection_item (collection_item_id);
CREATE INDEX collection_item_deleted_idx ON collection_item (deleted);
CREATE INDEX collection_item_resource_gooru_oid_idx_1 ON collection_item (resource_gooru_oid);



CREATE TABLE table_datatypes (
    key text,
    column1 text,
    value text,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE license (
    key text PRIMARY KEY,
    id bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';




CREATE TABLE content_meta (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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
    AND speculative_retry = 'NONE';



CREATE TABLE user_preference (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE job_config_settings (
    key text PRIMARY KEY,
    "D:yyyyMMdd~C:content" text,
    "OauthClient-Register" text,
    "OauthClient-Update" text,
    "OauthClient-list" text,
    "OauthClient-list-byOrganization" text,
    "SignOut" text,
    "User-Signin" text,
    "active~collection~plays" text,
    "active~resource~plays" text,
    "add-collaborators" text,
    "anonymous-login" text,
    "anonymous~session" text,
    "assignment-create" text,
    "assignment-delete" text,
    "assignment-item-add" text,
    "assignment-item-delete" text,
    "assignment-update" text,
    "assignment-view" text,
    "atmosphere.end.point" text,
    "attribution-search" text,
    "check-availability" text,
    "check-content-access" text,
    "check-content-permission" text,
    "check-session" text,
    "classpage-create" text,
    "classpage-create-collection-task-item" text,
    "classpage-delete" text,
    "classpage-get" text,
    "classpage-get-items" text,
    "classpage-get-my-workspace" text,
    "classpage-update" text,
    "classpage-view" text,
    "classpage.create" text,
    "classpage.user.add" text,
    "classpage.view" text,
    "clear-library-cache" text,
    "collection-acknowledgement" text,
    "collection-copy" text,
    "collection-create" text,
    "collection-delete" text,
    "collection-load" text,
    "collection-play" text,
    "collection-play-dots" text,
    "collection-publish" text,
    "collection-question-resource-play-dots" text,
    "collection-quiz-search" text,
    "collection-resource-explanation-play-dots" text,
    "collection-resource-hint-play-dots" text,
    "collection-resource-oe-explanation-play-dots" text,
    "collection-resource-oe-hit-play-dots" text,
    "collection-resource-oe-play-dots" text,
    "collection-resource-oe-save-dots" text,
    "collection-resource-play" text,
    "collection-resource-play-dots" text,
    "collection-resource-player" text,
    "collection-resource-question-play-dots" text,
    "collection-search" text,
    "collection-suggest-search" text,
    "collection-update" text,
    "collection.create" text,
    "collection.delete" text,
    "collection.play" text,
    "collection.resource.play" text,
    "collection.resource.save" text,
    "collections-played" text,
    constant_value text,
    "content-views" text,
    "create-collaborator" text,
    "create-flag" text,
    "create-folder" text,
    "create-rating" text,
    "create-reaction" text,
    "create-reaction-preview" text,
    "create-report" text,
    "create-session" text,
    "create-session-item" text,
    "create-subscription" text,
    "create-task" text,
    "create-taskResource-assocaition" text,
    "create-teacher-tip" text,
    "create-user" text,
    created_collections text,
    db_collection_view text,
    db_resource_preview text,
    db_search_performed text,
    default_limit int,
    "delete-collaborator" text,
    "delete-folder" text,
    "delete-rating" text,
    "delete-reaction" text,
    "delete-report" text,
    "delete-task" text,
    delimiter text,
    "get-collaborators" text,
    "get-users-shelf" text,
    "heart-beat" text,
    ip_address text,
    "item.create" text,
    "item.create~classpage" text,
    "item.create~shelf.collection" text,
    "item.delete" text,
    "item.search~collection" text,
    "item.search~resource" text,
    item_value text,
    job_status text,
    jobs_count bigint,
    last_processed_time text,
    "load-page" text,
    login text,
    "login-SSO" text,
    logout text,
    maximum_row_limit int,
    "move-collection-folder" text,
    "profile-status" text,
    "profile-visit" text,
    "profile.action" text,
    "question-create" text,
    "question-create-collection-item" text,
    "question-delete" text,
    "question-explanation-play-dots" text,
    "question-hint-play-dots" text,
    "question-oe-explanation-play-dots" text,
    "question-oe-hint-play-dots" text,
    "question-oe-play-dots" text,
    "question-oe-save-dots" text,
    "question-play-dots" text,
    "question-search" text,
    "question-update" text,
    "quiz-attempt-start" text,
    "quiz-create" text,
    "quiz-delete" text,
    "quiz-play" text,
    "quiz-preview" text,
    "quiz-search" text,
    "quiz-suggest-search" text,
    "reaction.create" text,
    "reaction.delete" text,
    "resource-add" text,
    "resource-collection-list" text,
    "resource-create-collection-item" text,
    "resource-delete" text,
    "resource-play-dots" text,
    "resource-player" text,
    "resource-preview" text,
    "resource-search" text,
    "resource-update" text,
    "resource-url-check-search" text,
    "resource.play" text,
    "resource.save" text,
    "resource.user.feedback" text,
    resourceplayerstart text,
    "resourceresource-search" text,
    "resources-played" text,
    "resources-search" text,
    "scollection-copy" text,
    "scollection-create" text,
    "scollection-delete" text,
    "scollection-folder-create" text,
    "scollection-folder-update" text,
    "scollection-get" text,
    "scollection-get-item" text,
    "scollection-get-items" text,
    "scollection-item-add" text,
    "scollection-item-copy" text,
    "scollection-item-delete" text,
    "scollection-item-metadata-update" text,
    "scollection-item-re-order" text,
    "scollection-item-update" text,
    "scollection-metadata-update" text,
    "scollection-myworkspace-get" text,
    "scollection-search" text,
    "scollection-update" text,
    search_performed text,
    "searchquery-search" text,
    "segment-add" text,
    "segment-update" text,
    "session-expired" text,
    "shelf-add-resource" text,
    "shelf-archive-visit" text,
    "shelf-create" text,
    "shelf-get-resources" text,
    "shelf-mark-default" text,
    "shelf-move-content" text,
    "shelf-name-list" text,
    "shelf-rename" text,
    "shelf-resource-delete" text,
    "shelf-update" text,
    "show-rating" text,
    "signIn-google-home" text,
    "signIn-google-login" text,
    "signUp-home" text,
    "signUp-login" text,
    "standard-search" text,
    "standardCode-search" text,
    status text,
    "taxonomy-resource-delete" text,
    "update-event-id" text,
    "update-resource-collection-item" text,
    "update-session" text,
    "update-session-item" text,
    "update-task" text,
    "user-authenticate" text,
    "user-loginAs" text,
    "user-search" text,
    "user.activity" text,
    "user.login" text,
    "user.register" text,
    "user~session" text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE dim_time (
    key text PRIMARY KEY,
    format_h text,
    format_h_12 text,
    format_hm text,
    format_hm_12 text,
    format_hms text,
    format_hms_12 text,
    hour text,
    hour_12 bigint,
    is_am boolean,
    is_business boolean,
    is_pm boolean,
    minute text,
    second bigint,
    time_h timestamp,
    time_h_id bigint,
    time_hm timestamp,
    time_hm_id text,
    time_hms timestamp,
    time_id bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX hour ON dim_time (hour);
CREATE INDEX minute ON dim_time (minute);
CREATE INDEX time_hm_id ON dim_time (time_hm_id);



CREATE TABLE anonymized_key_mapping (
    key text PRIMARY KEY,
    constant_value text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX anonymized_key_mapping_constant_value_idx ON anonymized_key_mapping (constant_value);



CREATE TABLE real_time_counter (
    key text,
    column1 text,
    value counter,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE micro_aggregation (
    key text PRIMARY KEY,
    dummy text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';




CREATE TABLE extracted_code (
    key text PRIMARY KEY,
    code_id bigint,
    concept text,
    concept_code_id bigint,
    course text,
    course_code_id bigint,
    depth int,
    lesson text,
    lesson_code_id bigint,
    subject text,
    subject_code_id bigint,
    topic text,
    topic_code_id bigint,
    unit text,
    unit_code_id bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE custom_fields_definition (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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
    AND speculative_retry = 'NONE';




CREATE TABLE dim_user (
    key text PRIMARY KEY,
    account_created_type text,
    account_type_id bigint,
    account_uid text,
    active int,
    added_by_system bigint,
    confirm_status bigint,
    deactivated_on text,
    email_sso text,
    external_id text,
    firstname text,
    gooru_uid text,
    identity_id int,
    idp_id int,
    import_code text,
    is_deleted int,
    last_login text,
    lastname text,
    license_version text,
    login_type text,
    mail_status bigint,
    organization_uid text,
    parent_id bigint,
    parent_uid text,
    primary_organization_uid text,
    reference_uid text,
    register_token text,
    registered_on text,
    role_id bigint,
    security_group_uid text,
    user_group_uid text,
    user_role text,
    username text,
    view_flag bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX external_id ON dim_user (external_id);
CREATE INDEX gooru_uid ON dim_user (gooru_uid);
CREATE INDEX di_username ON dim_user (username);



CREATE TABLE custom_fields_data (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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
    AND speculative_retry = 'NONE';



CREATE TABLE real_time_operation_config (
    key text PRIMARY KEY,
    aggregator_json text,
    updated_by text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE sessions (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE user_taxonomy_proficiency (
    key text PRIMARY KEY,
    user_uid text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX user_uid_utp ON user_taxonomy_proficiency (user_uid);


CREATE TABLE resource_type (
    key text PRIMARY KEY,
    id bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE classpage (
    key text PRIMARY KEY,
    active_flag int,
    classpage_code text,
    classpage_content_id bigint,
    classpage_gooru_oid text,
    deleted int,
    "endTime" timestamp,
    gooru_uid text,
    is_group_owner int,
    organization_uid text,
    result boolean,
    rkey text,
    user_group_code text,
    user_group_type text,
    user_group_uid text,
    username text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX classpage_content_id ON classpage (classpage_content_id);
CREATE INDEX classpage_gooru_oid ON classpage (classpage_gooru_oid);
CREATE INDEX deleted ON classpage (deleted);
CREATE INDEX gooru_uid_2 ON classpage (gooru_uid);
CREATE INDEX is_group_owner ON classpage (is_group_owner);
CREATE INDEX user_group_code ON classpage (user_group_code);
CREATE INDEX user_group_uid ON classpage (user_group_uid);
CREATE INDEX usernames ON classpage (username);

CREATE TABLE dim_date (
    key text PRIMARY KEY,
    date text,
    date_id bigint,
    day bigint,
    day_name text,
    day_of_week bigint,
    is_bank_holiday boolean,
    is_business_day boolean,
    is_public_holiday boolean,
    is_weekday boolean,
    is_weekend boolean,
    month bigint,
    month_date_id bigint,
    month_name text,
    quarter bigint,
    quarteryear text,
    week_date_id bigint,
    year bigint,
    year_date_id bigint,
    yearweek text,
    yearweek_week bigint,
    yearweek_year bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_sstable_size': '52428800', 'max_threshold': '32', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'min_threshold': '4'}
    AND compression = {'chunk_length_kb': '64', 'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX date ON dim_date (date);



CREATE TABLE search_setting (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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
    AND speculative_retry = 'NONE';



CREATE TABLE formula_detail (
    key text PRIMARY KEY,
    aggregate_type text,
    constant_value text,
    formula text,
    last_processed_time text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX aggregate_type_idx ON formula_detail (aggregate_type);
CREATE INDEX constant_value_idx ON formula_detail (constant_value);



CREATE TABLE session_activity_counter (
    key text,
    column1 text,
    value counter,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE class_activity_counter (
    key text,
    column1 text,
    value counter,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE calculate_view_count (
    key text,
    column1 text,
    value bigint,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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



CREATE TABLE live_dashboard (
    key text,
    column1 text,
    value counter,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE staging_event_detail (
    key text PRIMARY KEY,
    api_key text,
    class bigint,
    concept bigint,
    content_gooru_id text,
    course bigint,
    date_id bigint,
    district bigint,
    event_id text,
    event_name text,
    gooru_uid text,
    hour_id bigint,
    lesson bigint,
    month_id bigint,
    organization_uid text,
    parent_event_id text,
    parent_gooru_id text,
    resource_category_id bigint,
    resource_type_id bigint,
    school bigint,
    score bigint,
    session_id text,
    state bigint,
    subject bigint,
    topic bigint,
    total_timespent_ms bigint,
    unit bigint,
    week_id bigint,
    year_id bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE event_completion (
    key text PRIMARY KEY,
    "endTime" bigint,
    "eventName" text,
    "startTime" bigint
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE query_reports (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';




CREATE TABLE migrate_rating (
    key text PRIMARY KEY,
    gooru_rating bigint,
    gooru_ratingcount bigint,
    gooru_reviewcount bigint,
    insights_count_ratings bigint,
    insights_count_reviews bigint,
    insights_count_sum_rate bigint,
    insights_data text,
    live_status text,
    migration_rating_status text,
    migration_rating_user_count_status text,
    migration_review_status text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX insights_data_index ON migrate_rating (insights_data);
CREATE INDEX live_status_index ON migrate_rating (live_status);
CREATE INDEX data_index ON migrate_rating (migration_rating_status);
CREATE INDEX user_status_index ON migrate_rating (migration_rating_user_count_status);




CREATE TABLE user_group_association (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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




CREATE TABLE collection (
    key text PRIMARY KEY,
    collection_type text,
    content_id bigint,
    estimated_time text,
    goals text,
    gooru_oid text,
    grade text,
    key_points text,
    language text,
    narration_link text,
    network text,
    notes text,
    type bigint,
    version int
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX content_id ON collection (content_id);
CREATE INDEX collection_gooru_oid_idx ON collection (gooru_oid);



CREATE TABLE indexing_queue (
    key text PRIMARY KEY,
    dummy text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE user (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE instructional_use (
    key text PRIMARY KEY,
    id bigint
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE class (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
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



CREATE TABLE question_count (
    key text PRIMARY KEY,
    "collection_Content_id" bigint,
    "itemCount" bigint,
    "mcCount" bigint,
    "oeCount" bigint,
    "questionCount" bigint,
    "resourceCount" bigint
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = '99.0PERCENTILE';


CREATE TABLE user_event_index_queue (
    key text PRIMARY KEY,
    status text,
    time_taken bigint
) WITH COMPACT STORAGE
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
CREATE INDEX status ON user_event_index_queue (status);
CREATE INDEX time_taken ON user_event_index_queue (time_taken);



CREATE TABLE config_settings (
    key text PRIMARY KEY,
    config_name text,
    list_indexes text,
    session_token text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE real_time_counters (
    key blob,
    column1 text,
    value counter,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';




CREATE TABLE anonymized_data (
    key text PRIMARY KEY,
    firstname_alias text,
    lastname_alias text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';



CREATE TABLE extracted_user (
    key text PRIMARY KEY,
    class text,
    district text,
    grade text,
    "organizationUId" text,
    organization_uid text,
    school text,
    state text,
    tax_ids text,
    teacher text,
    user_type text,
    usertype text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';
CREATE INDEX extracted_user_organization_uid_idx ON extracted_user (organization_uid);




CREATE TABLE recent_viewed_resources (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';


 
