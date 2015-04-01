/*******************************************************************************
 * JobStatusDAOCassandraImpl.java
 * insights-event-logger
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.kafka.event.microaggregator.dao;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Caching;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

public class JobStatusDAOCassandraImpl extends BaseDAOCassandraImpl implements JobStatusDAOCassandra {
    private static final Logger logger = LoggerFactory.getLogger(JobStatusDAOCassandraImpl.class);
    private final ColumnFamily<String, String> jobStatusCF;
    private static final String CF_JOB_STATUS = "job_status";
   
    
    
    public JobStatusDAOCassandraImpl(CassandraConnectionProvider connectionProvider) {
        super(connectionProvider);
        jobStatusCF = new ColumnFamily<String, String>(
        		CF_JOB_STATUS, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
    }

    public void updateJobStatus(String currentStatus){
    	
    	 String KEY = "job~status";
    	 
    	 MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
         
         m.withRow(jobStatusCF, KEY)
                 .putColumn("job_status", currentStatus, null);
         try {
 	        m.execute();
 	    } catch (ConnectionException e) {
 	        logger.info("Error while inserting event data to cassandra", e);
 	    }
    }

    public void balancingJobsCount(long count){
    	
   	 String KEY = "running-jobs-count";
   	 
   	 MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        
        m.withRow(jobStatusCF, KEY)
                .putColumn("jobs_count", count, null);
        try {
	        m.execute();
	    } catch (ConnectionException e) {
	        logger.info("Error while inserting event data to cassandra", e);
	    }
   }

public String checkJobStatus(){

	ColumnList<String> jobStatus = null;
	String KEY = "job~status";
	try {
		jobStatus = getKeyspace().prepareQuery(jobStatusCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(KEY).execute().getResult();
	} catch (ConnectionException e) {
		// TODO Auto-generated catch block
		logger.error("Exception:"+e);
	}
	
	return jobStatus.getStringValue("job_status", null);

	
}

public long getJobsCount(){

	ColumnList<String> jobStatus = null;
	String KEY = "running-jobs-count";
	try {
		jobStatus = getKeyspace().prepareQuery(jobStatusCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(KEY).execute().getResult();
	} catch (ConnectionException e) {
		logger.error("Exception:"+e);
	}
	return jobStatus.getLongValue("jobs_count", 0L);

	
}

@Caching
public String getConstants(String KEY){

	String constantName = null;
	ColumnList<String> jobConstants = null;
	try {
		jobConstants = getKeyspace().prepareQuery(jobStatusCF).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(KEY).execute().getResult();
	} catch (ConnectionException e) {
		logger.error("Exception:"+e);
	}
	
	if(jobConstants != null){
		constantName = jobConstants.getStringValue("constant_value", null);
	}
	
	return constantName;
}
}
