package org.logger.event.cassandra.loader.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.ednovo.data.model.EventBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventsUpdateDAOImpl extends BaseDAOCassandraImpl implements EventsUpdateDAO {
	private static final Logger LOG = LoggerFactory.getLogger(EventsUpdateDAOImpl.class);

	private BaseCassandraRepo baseCassandraDao;

	private final ExecutorService service = Executors.newFixedThreadPool(10);

	public EventsUpdateDAOImpl() {
		baseCassandraDao = BaseCassandraRepo.instance();
	}

	@Override
	public void eventsHandler(final EventBuilder event) {
		try {
			Set<Callable<String>> tasks = new HashSet<Callable<String>>();
			tasks.add(new Callable<String>() {
				public String call() throws Exception {
					switch (event.getEventName()) {

					case Constants.COLLECTION_PLAY:
						handlePlayerEvents(event);
						saveStatIndexPublisherQueue(event);
						break;
					case Constants.COLLECTION_RESOURCE_PLAY:
						handlePlayerEvents(event);
						saveStatIndexPublisherQueue(event);
						break;
					case Constants.RESOURCE_PLAY:
						handlePlayerEvents(event);
						saveStatIndexPublisherQueue(event);
						break;
					case Constants.CLASS_JOIN:
						handleClassJoin(event);
						break;
					case Constants.ITEM_CREATE:
						handleItemCreate(event);
						break;
					case Constants.COLLABORATORS_UPDATE:
						handleCollaboratorsUpdate(event);
						break;
					case Constants.ITEM_COPY:
						handleItemCopy(event);
						break;
					case Constants.ITEM_DELETE:
						handleItemDelete(event);
						break;
					case Constants.ITEM_ADD:
						handleItemAdd(event);
						break;
					default:
						LOG.info("Nothing to process....");
					}
					return Constants.COMPLETED;
				}
			});

			List<Future<String>> status = service.invokeAll(tasks);
			for (Future<String> taskStatus : status) {
				LOG.info(taskStatus.get());
			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}

	private void handlePlayerEvents(final EventBuilder event) {
		
		if (event.getViews() > 0) {
			baseCassandraDao.incrementStatisticalCounterData(event.getContentGooruId(),Constants.VIEWS, event.getViews());
			baseCassandraDao.incrementUserStatisticalCounterData(event.getContentGooruId(), event.getGooruUUID(), Constants.VIEWS, event.getViews());
		}
		if (event.getTimespent() > 0) {
			baseCassandraDao.incrementStatisticalCounterData(event.getContentGooruId(), Constants.TOTALTIMEINMS, event.getTimespent());
			baseCassandraDao.incrementUserStatisticalCounterData(event.getContentGooruId(),event.getGooruUUID(), Constants.TOTALTIMEINMS, event.getTimespent());
		}
	}

	private void saveStatIndexPublisherQueue(final EventBuilder event){
		String resourceType = null;
		if(LoaderConstants.CPV1.getName().equalsIgnoreCase(event.getEventName())){
			resourceType = event.getCollectionType();
		}else{
			resourceType = event.getResourceType();
		}
		baseCassandraDao.addStatPublisherQueue(Constants.PUBLISH_METRICS, event.getContentGooruId(), resourceType, event.getEventTime());
	}
	
	private void handleClassJoin(final EventBuilder event) {
		HashSet<String> students = new HashSet<String>();
		students.add(event.getGooruUUID());
 		baseCassandraDao.saveClassMembers(event.getContentGooruId(), students);
	}

	private void handleCollaboratorsUpdate(final EventBuilder event) {
		try {
			HashSet<String> collaborators = new HashSet<String>();
			JSONObject data = event.getPayLoadObject().getJSONObject(Constants.DATA);
			if (data != null) {
				JSONArray collaboratorsArray = data.getJSONArray(Constants.COLLABORATORS);
				if (collaboratorsArray != null) {
					for (int index = 0; index < (collaboratorsArray).length(); index++) {
						collaborators.add(collaboratorsArray.getString(index));
					}
					baseCassandraDao.updateCollaborators(event.getContentGooruId(), collaborators);
					baseCassandraDao.balanceCounterData(event.getContentGooruId(), Constants.COLLABORATORS, ((Number) collaborators.size()).longValue());
					baseCassandraDao.balanceUserCounterData(event.getContentGooruId(), Constants.USER, Constants.COLLABORATORS, ((Number) collaborators.size()).longValue());
				}
			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}

	private void handleItemDelete(final EventBuilder event) {
		try {
			String parentContentIdD = event.getContext().getString(Constants.PARENT_CONTENT_ID);
			if (event.getContentFormat().matches(Constants.RESOURCE_FORMATS)) {
				baseCassandraDao.decrementStatisticalCounterData(parentContentIdD,Constants.USED_IN_COLLECTION_COUNT, 1);
				baseCassandraDao.decrementUserStatisticalCounterData(parentContentIdD,event.getGooruUUID(), Constants.USED_IN_COLLECTION_COUNT, 1);
			}
			baseCassandraDao.decrementStatisticalCounterData(parentContentIdD, Constants.COPY, 1);
			baseCassandraDao.decrementUserStatisticalCounterData(parentContentIdD,event.getGooruUUID(), Constants.COPY, 1);
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}

	private void handleItemCopy(final EventBuilder event) {
		String parentContentId;
		try {
			parentContentId = event.getPayLoadObject().getJSONObject(Constants.TARGET).getString(Constants.PARENT_CONTENT_ID);
			if (event.getContentFormat().matches(Constants.RESOURCE_FORMATS)) {
				baseCassandraDao.incrementStatisticalCounterData(parentContentId, Constants.USED_IN_COLLECTION_COUNT, 1);
				baseCassandraDao.incrementUserStatisticalCounterData(parentContentId,event.getGooruUUID(), Constants.USED_IN_COLLECTION_COUNT, 1);
			}
			baseCassandraDao.incrementStatisticalCounterData(parentContentId, Constants.COPY, 1);
			baseCassandraDao.incrementUserStatisticalCounterData(parentContentId, event.getGooruUUID(), Constants.COPY, 1);
			
		} catch (Exception e) {
			LOG.error("Exception : ", e);
		}
	}

	private void handleItemAdd(final EventBuilder event) {
		try {
			String parentContentIdA = event.getPayLoadObject().getJSONObject(Constants.TARGET).getString(Constants.PARENT_CONTENT_ID);
			if (event.getContentFormat().matches(Constants.RESOURCE_FORMATS)) {
				baseCassandraDao.incrementStatisticalCounterData(parentContentIdA, Constants.USED_IN_COLLECTION_COUNT, 1);
				baseCassandraDao.incrementUserStatisticalCounterData(parentContentIdA, event.getGooruUUID(), Constants.USED_IN_COLLECTION_COUNT, 1);
			}
		} catch (Exception e) {
			LOG.error("Exception : ", e);
		}
	}

	private void handleItemCreate(final EventBuilder event) {
		if (event.getContentFormat().equalsIgnoreCase(Constants.CLASS) || event.getContentFormat().matches(Constants.COLLECTION_TYPES)) {
			baseCassandraDao.updateContentCreators(event.getContentGooruId(), event.getGooruUUID());
		}
	}
}
