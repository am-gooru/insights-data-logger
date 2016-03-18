package org.logger.event.cassandra.loader.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;


public class MastryGenerator implements Runnable {

	private BaseCassandraRepo baseCassandraDao;
	
	private ContentTaxonomyActivity contentTaxonomyActivity;
	
	private static Logger logger = LoggerFactory.getLogger(ClassActivityDataCubeGenerator.class);
	
	public MastryGenerator(ContentTaxonomyActivity contentTaxonomyActivity, BaseCassandraRepo baseCassandraDao) {
		this.contentTaxonomyActivity =  contentTaxonomyActivity;
		this.baseCassandraDao = baseCassandraDao;
	}

	public void run() {
		try {
			if(contentTaxonomyActivity != null && contentTaxonomyActivity.getTaxonomyIds() != null){
				ContentTaxonomyActivity contentTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				ContentTaxonomyActivity contentClassTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				TaxonomyActivityDataCube taxonomyActivityDataCube = new TaxonomyActivityDataCube();
				Map<String, String> contentTaxKeyColumnPair = new HashMap<String, String>();
				
				for (int index = 0 ;  index < (contentTaxonomyActivity.getTaxonomyIds()).length(); index++) {
				ResultSet taxRows = baseCassandraDao.getTaxonomy(contentTaxonomyActivity.getTaxonomyIds().getString(index));
				if (taxRows != null) {
					for (com.datastax.driver.core.Row taxColumns : taxRows) {
						contentTaxonomyActivityInstance.setSubjectId(taxColumns.getString(Constants.SUBJECT_ID));
						contentTaxonomyActivityInstance.setCourseId(taxColumns.getString(Constants.COURSE_ID));
						contentTaxonomyActivityInstance.setDomainId(taxColumns.getString(Constants.DOMAIN_ID));
						contentTaxonomyActivityInstance.setStandardsId(taxColumns.getString(Constants.STANDARDS_ID));
						contentTaxonomyActivityInstance.setLearningTargetsId(taxColumns.getString(Constants.LEARNING_TARGETS_ID));
						ResultSet taxActivityRows = baseCassandraDao.getContentTaxonomyActivity(contentTaxonomyActivityInstance);
						if (taxActivityRows != null) {
							for (com.datastax.driver.core.Row taxActivityColumns : taxActivityRows) {
								contentTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getViews() + taxActivityColumns.getLong(Constants.VIEWS));
								contentTaxonomyActivityInstance.setTimeSpent(contentTaxonomyActivity.getTimeSpent() + taxActivityColumns.getLong(Constants._TIME_SPENT));
							}
						}
						contentTaxonomyActivityInstance.setScore(contentTaxonomyActivity.getScore());
						baseCassandraDao.saveContentTaxonomyActivity(contentTaxonomyActivityInstance);
						
						/**
						 * content_class_taxonomy_activity store
						 */
						contentClassTaxonomyActivityInstance.setSubjectId(taxColumns.getString(Constants.SUBJECT_ID));
						contentClassTaxonomyActivityInstance.setCourseId(taxColumns.getString(Constants.COURSE_ID));
						contentClassTaxonomyActivityInstance.setDomainId(taxColumns.getString(Constants.DOMAIN_ID));
						contentClassTaxonomyActivityInstance.setStandardsId(taxColumns.getString(Constants.STANDARDS_ID));
						contentClassTaxonomyActivityInstance.setLearningTargetsId(taxColumns.getString(Constants.LEARNING_TARGETS_ID));
						ResultSet classTaxActivityRows = baseCassandraDao.getContentClassTaxonomyActivity(contentClassTaxonomyActivityInstance);
						if (classTaxActivityRows != null) {
							for (com.datastax.driver.core.Row classTaxActivityColumns : classTaxActivityRows) {
								contentClassTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getViews() + classTaxActivityColumns.getLong(Constants.VIEWS));
								contentClassTaxonomyActivityInstance.setTimeSpent(contentTaxonomyActivity.getTimeSpent() + classTaxActivityColumns.getLong(Constants._TIME_SPENT));
								contentClassTaxonomyActivityInstance.setScore(contentTaxonomyActivity.getScore());
								baseCassandraDao.saveContentClassTaxonomyActivity(contentClassTaxonomyActivityInstance);
							}
						}
						taxonomyActivityDataCube.setRowKey(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(),contentTaxonomyActivity.getLearningTargetsId()));
						taxonomyActivityDataCube.setLeafNode(contentTaxonomyActivity.getGooruOid());
						//generateDataCubeObj(taxonomyActivityDataCube);
						//baseCassandraDao.saveTaxonomyActivityDataCube(taxonomyActivityDataCube);
						
						taxonomyActivityDataCube.setRowKey(appendTilda(contentTaxonomyActivity.getClassUid(),contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(),contentTaxonomyActivity.getLearningTargetsId()));
						//generateDataCubeObj(taxonomyActivityDataCube);
						//baseCassandraDao.saveTaxonomyActivityDataCube(taxonomyActivityDataCube);
						
						keyPairGenerator(contentTaxKeyColumnPair, contentTaxonomyActivityInstance);
					}
				}
			}
				//generatedDataCube(contentTaxKeyColumnPair);
			}
		} catch (Exception e) {
			logger.error("Exception while generate Mastery data", e);
		}
		
	}
	
	private void keyPairGenerator(Map<String, String> contentTaxKeyColumnPair, ContentTaxonomyActivity contentTaxonomyActivity) {
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(),contentTaxonomyActivity.getLearningTargetsId()), contentTaxonomyActivity.getGooruOid());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId()), contentTaxonomyActivity.getLearningTargetsId());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId()), contentTaxonomyActivity.getStandardsId());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId()), contentTaxonomyActivity.getDomainId());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId()), contentTaxonomyActivity.getCourseId());
		
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(),contentTaxonomyActivity.getLearningTargetsId()), contentTaxonomyActivity.getGooruOid());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId()), contentTaxonomyActivity.getLearningTargetsId());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId()), contentTaxonomyActivity.getStandardsId());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId()), contentTaxonomyActivity.getDomainId());
		contentTaxKeyColumnPair.put(appendTilda(contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId()),contentTaxonomyActivity.getCourseId());
	}

	private TaxonomyActivityDataCube generateDataCubeObj(TaxonomyActivityDataCube taxonomyActivityDataCube) {
		ResultSet taxActivityDataCubeRows = baseCassandraDao.getContentTaxonomyActivityDataCube(taxonomyActivityDataCube.getRowKey(), taxonomyActivityDataCube.getLeafNode());
		if (taxActivityDataCubeRows != null) {
			for (com.datastax.driver.core.Row taxActivityDataCubeColumn : taxActivityDataCubeRows) {
				taxonomyActivityDataCube.setRowKey(taxonomyActivityDataCube.getRowKey());
				taxonomyActivityDataCube.setLeafNode(taxonomyActivityDataCube.getLeafNode());
				if (contentTaxonomyActivity.getResourceType().equalsIgnoreCase(Constants.RESOURCE)) {
					taxonomyActivityDataCube.setViews(contentTaxonomyActivity.getViews() + taxActivityDataCubeColumn.getLong(Constants.VIEWS));
					taxonomyActivityDataCube.setAttempts(0L + taxActivityDataCubeColumn.getLong(Constants.ATTEMPTS));
					taxonomyActivityDataCube.setResourceTimespent(contentTaxonomyActivity.getTimeSpent() + taxActivityDataCubeColumn.getLong("resource_timespent"));
					taxonomyActivityDataCube.setQuestionTimespent(0L+taxActivityDataCubeColumn.getLong("question_timespent"));
				} else {
					taxonomyActivityDataCube.setViews(0L + taxActivityDataCubeColumn.getLong(Constants.VIEWS));
					taxonomyActivityDataCube.setAttempts(contentTaxonomyActivity.getViews() + taxActivityDataCubeColumn.getLong(Constants.ATTEMPTS));
					taxonomyActivityDataCube.setResourceTimespent(0L+taxActivityDataCubeColumn.getLong("resource_timespent"));
					taxonomyActivityDataCube.setQuestionTimespent(contentTaxonomyActivity.getTimeSpent()+taxActivityDataCubeColumn.getLong("question_timespent"));
				}
				taxonomyActivityDataCube.setScore(contentTaxonomyActivity.getScore());
			}
		}

		return taxonomyActivityDataCube;
	}
	
	private void generatedDataCube(Map<String, String> keyColumnPair){
		for (Map.Entry<String, String> entry : keyColumnPair.entrySet())
		{
			TaxonomyActivityDataCube taxonomyActivityDataCube = new TaxonomyActivityDataCube();
			taxonomyActivityDataCube.setRowKey(entry.getKey());
			taxonomyActivityDataCube.setLeafNode(entry.getValue());
			generateDataCubeObj(taxonomyActivityDataCube);
			taxonomyActivityDataCube.setScore(baseCassandraDao.getContentTaxonomyActivityScore(entry.getKey()));
			baseCassandraDao.saveTaxonomyActivityDataCube(taxonomyActivityDataCube);
		}
	}
	
	private String appendTilda(String... columns) {
		StringBuilder columnKey = new StringBuilder();
		for (String column : columns) {
			if (StringUtils.isNotBlank(column)) {
				columnKey.append(columnKey.length() > 0 ? Constants.SEPERATOR : Constants.EMPTY);
				columnKey.append(column);
			}
		}
		return columnKey.toString();

	}

}
