package org.logger.event.cassandra.loader.dao;

import org.ednovo.data.model.ContentTaxonomyActivity;
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
			for (String taxId : contentTaxonomyActivity.getTaxonomyIds()) {
				ContentTaxonomyActivity contentTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				ContentTaxonomyActivity contentClassTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				Rows<String, String> taxRows = baseCassandraDao.getTaxonomy(taxId);
				if (taxRows != null && taxRows.size() > 0) {
					for (Row<String, String> taxRow : taxRows) {
						ColumnList<String> taxColumns = taxRow.getColumns();
						contentTaxonomyActivityInstance.setSubjectId(taxColumns.getStringValue(SUBJECT, NA));
						contentTaxonomyActivityInstance.setCourseId(taxColumns.getStringValue(COURSE, NA));
						contentTaxonomyActivityInstance.setDomainId(taxColumns.getStringValue(DOMAIN, NA));
						contentTaxonomyActivityInstance.setSubDomainId(taxColumns.getStringValue(SUB_DOMAIN, NA));
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
						contentClassTaxonomyActivityInstance.setSubDomainId(taxColumns.getStringValue(SUB_DOMAIN, NA));
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
					}
				}
			}
			}
		} catch (Exception e) {
			logger.error("Exception while generate Mastery data", e);
		}
		
	}

}
