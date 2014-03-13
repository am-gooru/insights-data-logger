CREATE TABLE activity_stream (
  KEY text PRIMARY KEY,
  username text
) WITH
  comment='' AND
  comparator=text AND
  read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  default_validation=blob AND
  min_compaction_threshold=4 AND
  max_compaction_threshold=32 AND
  replicate_on_write='true' AND
  compaction_strategy_class='SizeTieredCompactionStrategy' AND
  compression_parameters:sstable_compression='SnappyCompressor';
  
  CREATE INDEX gooru_uid ON dim_user (gooru_uid);