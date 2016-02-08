package org.kafka.event.microaggregator.dao;

import java.util.Map;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.Constants;
import org.kafka.event.microaggregator.core.LoaderConstants;

public class UpdateRawDataDAOImpl extends BaseDAOCassandraImpl implements UpdateRawDataDAO,Constants{

	private CassandraConnectionProvider connectionProvider;
    
    private CollectionItemDAOImpl collectionItem;
    
    private EventDetailDAOCassandraImpl eventDetailDao; 
    
    private DimResourceDAOImpl dimResource;
    
    private ClasspageDAOImpl classpage;
    
    private CollectionDAOImpl collection;
    
	public UpdateRawDataDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.collectionItem = new CollectionItemDAOImpl(this.connectionProvider);
        this.eventDetailDao = new EventDetailDAOCassandraImpl(this.connectionProvider);
        this.dimResource = new DimResourceDAOImpl(this.connectionProvider);
        this.classpage = new ClasspageDAOImpl(this.connectionProvider);
	}

	public void updateRawData(Map<String,String> eventMap){

		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CLPCV1.getName())){
			classpage.updateClasspage(eventMap);
		}
		if(eventMap.get(EVENTNAME).equalsIgnoreCase(LoaderConstants.CCV1.getName())){
			collection.updateCollection(eventMap);
			collectionItem.updateCollectionItem(eventMap);
		}
	}
}
