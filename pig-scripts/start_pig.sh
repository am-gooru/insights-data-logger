datestamp=`date '+%Y-%m-%d' --date="1 days ago"`
/usr/local/pig/bin/pig_cassandra -x local  -param DATE=$datestamp /home/hduser/event-api/pig-scripts/world_aggregation_from_hourly_to_all.pig

