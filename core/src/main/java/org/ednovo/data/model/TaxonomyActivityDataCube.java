package org.ednovo.data.model;

public class TaxonomyActivityDataCube {

	private String rowKey;
	
	private String leafNode;
	
	private long attempts;
	
	private long views;
	
	private long questionTimespent;
	
	private long resourceTimespent;
	
	private long score;

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

	public long getAttempts() {
		return attempts;
	}

	public void setAttempts(long attempts) {
		this.attempts = attempts;
	}

	public long getViews() {
		return views;
	}

	public void setViews(long views) {
		this.views = views;
	}

	public long getQuestionTimespent() {
		return questionTimespent;
	}

	public void setQuestionTimespent(long questionTimespent) {
		this.questionTimespent = questionTimespent;
	}

	public long getResourceTimespent() {
		return resourceTimespent;
	}

	public void setResourceTimespent(long resourceTimespent) {
		this.resourceTimespent = resourceTimespent;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}
}
