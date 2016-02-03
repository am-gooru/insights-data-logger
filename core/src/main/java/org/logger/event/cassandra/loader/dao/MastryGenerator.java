package org.logger.event.cassandra.loader.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;


public class MastryGenerator implements Runnable, Constants {

	private BaseCassandraRepoImpl baseCassandraDao;
	
	private ContentTaxonomyActivity contentTaxonomyActivity;
	
	private static Logger logger = LoggerFactory.getLogger(ClassActivityDataCubeGenerator.class);
	
	public MastryGenerator(ContentTaxonomyActivity contentTaxonomyActivity, BaseCassandraRepoImpl baseCassandraDao) {
		this.contentTaxonomyActivity =  contentTaxonomyActivity;
		this.baseCassandraDao = baseCassandraDao;
	}

	@Override
	public void run() {
		try {
			if(contentTaxonomyActivity != null && contentTaxonomyActivity.getTaxonomyIds() != null){
				ContentTaxonomyActivity contentTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				ContentTaxonomyActivity contentClassTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				TaxonomyActivityDataCube taxonomyActivityDataCube = new TaxonomyActivityDataCube();
				Map<String, String> contentTaxKeyColumnPair = new HashMap<String, String>();
				
				for (String taxId : contentTaxonomyActivity.getTaxonomyIds()) {
				Rows<String, String> taxRows = baseCassandraDao.getTaxonomy(taxId);
				if (taxRows != null && taxRows.size() > 0) {
					for (Row<String, String> taxRow : taxRows) {
						ColumnList<String> taxColumns = taxRow.getColumns();
						contentTaxonomyActivityInstance.setSubjectId(taxColumns.getStringValue(SUBJECT, NA));
						contentTaxonomyActivityInstance.setCourseId(taxColumns.getStringValue(COURSE, NA));
						contentTaxonomyActivityInstance.setDomainId(taxColumns.getStringValue(DOMAIN, NA));
						contentTaxonomyActivityInstance.setStandardsId(taxColumns.getStringValue(STANDARDS, NA));
						contentTaxonomyActivityInstance.setLearningTargetsId(taxColumns.getStringValue(LEARNING_TARGETS, NA));
						Rows<String, String> taxActivityRows = baseCassandraDao.getContentTaxonomyActivity(contentTaxonomyActivityInstance);
						if (taxActivityRows != null && taxActivityRows.size() > 0) {
							for (Row<String, String> taxActivityRow : taxActivityRows) {
								ColumnList<String> taxActivityColumns = taxActivityRow.getColumns();
								contentTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getViews() + taxActivityColumns.getLongValue(VIEWS, 0L));
								contentTaxonomyActivityInstance.setTimeSpent(contentTaxonomyActivity.getTimeSpent() + taxActivityColumns.getLongValue(_TIME_SPENT, 0L));
								contentTaxonomyActivityInstance.setScore(contentTaxonomyActivity.getScore());
								baseCassandraDao.saveContentTaxonomyActivity(contentTaxonomyActivityInstance);
							}
						}
						
						/**
						 * content_class_taxonomy_activity store
						 */
						contentClassTaxonomyActivityInstance.setSubjectId(taxColumns.getStringValue(SUBJECT, NA));
						contentClassTaxonomyActivityInstance.setCourseId(taxColumns.getStringValue(COURSE, NA));
						contentClassTaxonomyActivityInstance.setDomainId(taxColumns.getStringValue(DOMAIN, NA));
						contentClassTaxonomyActivityInstance.setStandardsId(taxColumns.getStringValue(STANDARDS, NA));
						contentClassTaxonomyActivityInstance.setLearningTargetsId(taxColumns.getStringValue(LEARNING_TARGETS, NA));
						Rows<String, String> classTaxActivityRows = baseCassandraDao.getContentClassTaxonomyActivity(contentClassTaxonomyActivityInstance);
						if (classTaxActivityRows != null && classTaxActivityRows.size() > 0) {
							for (Row<String, String> classTaxActivityRow : classTaxActivityRows) {
								ColumnList<String> classTaxActivityColumns = classTaxActivityRow.getColumns();
								contentClassTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getViews() + classTaxActivityColumns.getLongValue(VIEWS, 0L));
								contentClassTaxonomyActivityInstance.setTimeSpent(contentTaxonomyActivity.getTimeSpent() + classTaxActivityColumns.getLongValue(_TIME_SPENT, 0L));
								contentClassTaxonomyActivityInstance.setScore(contentTaxonomyActivity.getScore());
								baseCassandraDao.saveContentClassTaxonomyActivity(contentClassTaxonomyActivityInstance);
							}
						}
						taxonomyActivityDataCube.setRowKey(appendTilda(contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(),contentTaxonomyActivity.getLearningTargetsId()));
						taxonomyActivityDataCube.setLeafNode(contentTaxonomyActivity.getGooruOid());
						generateDataCubeObj(taxonomyActivityDataCube);
						baseCassandraDao.saveTaxonomyActivityDataCube(taxonomyActivityDataCube);
						
						taxonomyActivityDataCube.setRowKey(appendTilda(contentTaxonomyActivity.getClassUid(),contentTaxonomyActivity.getUserUid(),contentTaxonomyActivity.getSubjectId(),contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(),contentTaxonomyActivity.getLearningTargetsId()));
						generateDataCubeObj(taxonomyActivityDataCube);
						baseCassandraDao.saveTaxonomyActivityDataCube(taxonomyActivityDataCube);
						
						keyPairGenerator(contentTaxKeyColumnPair, contentTaxonomyActivityInstance);
					}
				}
			}
				generatedDataCube(contentTaxKeyColumnPair);
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
		Rows<String, String> taxActivityDataCubeRows = baseCassandraDao.getContentTaxonomyActivityDataCube(taxonomyActivityDataCube.getRowKey(), taxonomyActivityDataCube.getLeafNode());
		if (taxActivityDataCubeRows != null && taxActivityDataCubeRows.size() > 0) {
			for (Row<String, String> taxActivityDataCubeRow : taxActivityDataCubeRows) {
				ColumnList<String> taxActivityDataCubeColumn = taxActivityDataCubeRow.getColumns();
				taxonomyActivityDataCube.setRowKey(taxonomyActivityDataCube.getRowKey());
				taxonomyActivityDataCube.setLeafNode(taxonomyActivityDataCube.getLeafNode());
				if (contentTaxonomyActivity.getResourceType().equalsIgnoreCase(RESOURCE)) {
					taxonomyActivityDataCube.setViews(contentTaxonomyActivity.getViews() + taxActivityDataCubeColumn.getLongValue(VIEWS, 0L));
					taxonomyActivityDataCube.setAttempts(0L + taxActivityDataCubeColumn.getLongValue(ATTEMPTS, 0L));
					taxonomyActivityDataCube.setResourceTimespent(contentTaxonomyActivity.getTimeSpent() + taxActivityDataCubeColumn.getLongValue("resource_timespent", 0L));
					taxonomyActivityDataCube.setQuestionTimespent(0L+taxActivityDataCubeColumn.getLongValue("question_timespent", 0L));
				} else {
					taxonomyActivityDataCube.setViews(0L + taxActivityDataCubeColumn.getLongValue(VIEWS, 0L));
					taxonomyActivityDataCube.setAttempts(contentTaxonomyActivity.getViews() + taxActivityDataCubeColumn.getLongValue(ATTEMPTS, 0L));
					taxonomyActivityDataCube.setResourceTimespent(0L+taxActivityDataCubeColumn.getLongValue("resource_timespent", 0L));
					taxonomyActivityDataCube.setQuestionTimespent(contentTaxonomyActivity.getTimeSpent()+taxActivityDataCubeColumn.getLongValue("question_timespent", 0L));
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
				columnKey.append(columnKey.length() > 0 ? SEPERATOR : EMPTY);
				columnKey.append(column);
			}
		}
		return columnKey.toString();

	}

}
