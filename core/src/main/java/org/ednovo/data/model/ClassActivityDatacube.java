package org.ednovo.data.model;

import java.io.Serializable;

public class ClassActivityDatacube implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String rowKey;
	
	private String leafNode;
	
	private String userUid;
	
	private String collectionType;
	
	private long views;

	private long timeSpent;

	private long score;

	private long reaction;

	private long completedCount;
	
	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getLeafNode() {
		return leafNode;
	}

	public void setLeafNode(String leafNode) {
		this.leafNode = leafNode;
	}

	public String getUserUid() {
		return userUid;
	}

	public void setUserUid(String userUid) {
		this.userUid = userUid;
	}

	public String getCollectionType() {
		return collectionType;
	}

	public void setCollectionType(String collectionType) {
		this.collectionType = collectionType;
	}

	public long getViews() {
		return views;
	}

	public void setViews(long views) {
		this.views = views;
	}

	public long getTimeSpent() {
		return timeSpent;
	}

	public void setTimeSpent(long timeSpent) {
		this.timeSpent = timeSpent;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}

	public long getCompletedCount() {
		return completedCount;
	}

	public void setCompletedCount(long completedCount) {
		this.completedCount = completedCount;
	}

	public long getReaction() {
		return reaction;
	}

	public void setReaction(long reaction) {
		this.reaction = reaction;
	}
	
	
}
