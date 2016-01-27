package org.logger.event.cassandra.loader.dao;

import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualGrading implements Runnable, Constants{

	private BaseCassandraRepoImpl baseCassandraDao;
	
	private String questionId;
	
	private String userId;
	
	private String teacherId;
	
	private String sessionId;
	
	private long score;
	
	private static Logger logger = LoggerFactory.getLogger(ClassActivityDataCubeGenerator.class);
	
	public ManualGrading(BaseCassandraRepoImpl baseCassandraDao, String questionId, String userId, String teacherId, String sessionId, long score) {
		this.baseCassandraDao = baseCassandraDao;
		this.questionId = questionId;
		this.userId = userId;
		this.teacherId = teacherId;
		this.sessionId = sessionId;
		this.score = score;
	}
	@Override
	public void run() {
		// TODO : Logics to add
	}

}
