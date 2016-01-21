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
			for (String taxId : contentTaxonomyActivity.getTaxonomyIds()) {
				ContentTaxonomyActivity contentTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
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
						Rows<String, String> taxActivityRows = baseCassandraDao.getTaxonomyActivity(contentTaxonomyActivityInstance);
						if (taxActivityRows != null && taxActivityRows.size() > 0) {
							for (Row<String, String> taxActivityRow : taxActivityRows) {
								ColumnList<String> taxActivityColumns = taxActivityRow.getColumns();
								contentTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getViews() + taxActivityColumns.getLongValue(VIEWS, 0L));
								contentTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getTimeSpent() + taxActivityColumns.getLongValue(_TIME_SPENT, 0L));
								contentTaxonomyActivityInstance.setScore(contentTaxonomyActivity.getScore());
								baseCassandraDao.saveContentTaxonomyActivity(contentTaxonomyActivityInstance);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("Exception while generate Mastry data.");
		}
		
	}

}
