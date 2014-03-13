#!/bin/bash
#This script will start Pig and aggregate by Event Name
#To Run this shell script sh filename.sh <eventName> <hadoopMode> <Keyspace> <Loader ip>
#Sample command : sh Pig_Aggregation.sh collection-play-dots mapreduce insights_qa qa.logapi.goorulearning.org
###########################################################################################################################################################
export PIG_HOME="/usr/local/pig"
export JAVA_HOME="/usr/lib/jvm/java-7-oracle"
export PIG_INPUT_INITIAL_ADDRESS="162.243.131.194"
export PIG_INITIAL_ADDRESS="162.243.131.194"
export PIG_PARTITIONER="org.apache.cassandra.dht.RandomPartitioner"
export PIG_RPC_PORT="9160"
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/conf
###########################################################################################################################################################
START=$(date +%s);
END_TIME=`date '+%Y%m%d%H%M' --date='1 day'`;
HOST='198.199.93.161';
LOCATION='/home/gooruapp/event-logger-stable-1.1/';
CUSTOM_EVENT=$1;
MODE=$2;
KEYSPACE=$3;
LOADER_IP=$4;
SCRIPT_NAME=$1;
MAIL_SENT_TO='arokiadaniel.a@goorulearning.org,shanmuga.s@goorulearning.org,vijay@goorulearning.org,venkat@goorulearning.org,kumaraswamy@goorulearning.org,dhanasekar@goorulearning.org';
PIG_SCRIPT_LOCATION='/home/hduser/dash-board-aggregation';
LAST_PROCESSED=`date '+%Y%m%d%H%M' --date='300 minute ago'`;
START_TIME=`cat $SCRIPT_NAME.pid`;
if [ $? -gt 0 ]
then
  START_TIME=`date '+%Y%m%d%H%M' --date='1 day ago'`;
fi
###########################################################################################################################################################

echo 'Loader is running';

#Loading Stagin Data
ssh gooruapp@$LOADER_IP sh /home/gooruapp/event-logger-stable-1.1/loader/runs-loader-custom.sh ${START_TIME} ${END_TIME} ${HOST} ${LOCATION} ${CUSTOM_EVENT}

#Aggregation Starts Here

  if [ $? -gt 0 ]
  then
    echo "Hey! Loader is failed while loads "$START_TIME"-"$END_TIME" Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  exit;
  fi

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllStagEvents.pig

  if [ $? -gt 0 ]
  then
    echo "Hey! Staging to Hourly Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  exit;
  fi

if [ "$CUSTOM_EVENT" = "collection-play-dots" ]
then

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllCollectionPlay.pig

    if [ $? -gt 0 ]
    then
      echo "Hey! Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
    exit;
  fi
fi


if [ "$CUSTOM_EVENT" = "collection-resource-play-dots" ]
then

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllCollectionResourcePlay.pig

    if [ $? -gt 0 ]
    then
      echo "Hey! Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
    exit;
  fi
fi


if [ "$CUSTOM_EVENT" = "collection-question-resource-play-dots" ]
then
  
  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllQuestionStagEvents.pig

  if [ $? -gt 0 ]
  then
    echo "Hey! Staging to Hourly Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  exit;
  fi

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllCollectionQuestionResourcePlay.pig

    if [ $? -gt 0 ]
    then
      echo "Hey! Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
    exit;
  fi

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/CollectionGradeCalculation.pig

  if [ $? -gt 0 ]
    then
    echo "Hey! Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
    exit;
  fi

fi


if [ "$CUSTOM_EVENT" = "create-reaction" ]
then

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllCollectionResourceReactionCount.pig

    if [ $? -gt 0 ]
    then
     echo "Hey! Aggregation for "$CUSTOM_EVENT" [CollectionResourceReactionCount] is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
    exit;
  fi
  
  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllCollectionReactionCount.pig

  if [ $? -gt 0 ]
  then
    echo "Hey! Aggregation for "$CUSTOM_EVENT" [CollectionReactionCount] is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  fi

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllCollectionAvgReaction.pig

  if [ $? -gt 0 ]
  then
    echo "Hey! Aggregation for "$CUSTOM_EVENT" [CollectionAvgReaction] is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  fi

fi


if [ "$CUSTOM_EVENT" = "collection-resource-oe-play-dots" ]
then

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllOpenEndedResources.pig

  if [ $? -gt 0 ]
  then
    echo "Hey! Aggregation for "$CUSTOM_EVENT" is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  exit;
  fi

  $PIG_HOME/bin/pig_cassandra -x $MODE -param KEYSPACE=$KEYSPACE $PIG_SCRIPT_LOCATION/AllOpenEndedResourcesCount.pig

  if [ $? -gt 0 ]
  then
    echo "Hey! Aggregation for "$CUSTOM_EVENT" count is failed. Script name is : "$SCRIPT_NAME | mail -s "PIG Aggregation Aborted" $MAIL_SENT_TO
  exit;
  fi

fi

echo $LAST_PROCESSED > $SCRIPT_NAME.pid

END=$(date +%s);
DIFF=$(( $END - $START ));
echo $CUSTOM_EVENT":"$DIFF" seconds";
