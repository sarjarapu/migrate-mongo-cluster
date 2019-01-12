package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.migratecluster.model.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: CollectionFlowable
 * Author: shyam.arjarapu
 * Date: 4/26/17 6:40 PM
 * Description:
 */
public class CollectionFlowable extends Observable<Resource> {

    private final static Logger logger = LoggerFactory.getLogger(CollectionFlowable.class);
    private final MongoClient client;
    private final String databaseName;

    public CollectionFlowable(MongoClient client, String databaseName) {
        this.databaseName = databaseName;
        this.client = client;
    }


    @Override
    protected void subscribeActual(Observer<? super Resource> subscriber) {
        MongoDatabase database = client.getDatabase(databaseName);

        ListCollectionsIterable<Document> cursor = database.listCollections();
        MongoCursor<Document> iterator = cursor.iterator();

        while(iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                //logger.info("... found collection: {}.{}", database.getName(), item.getString("name"));
                //logger.debug(item.toJson());
                Resource resource = new Resource(databaseName,
                        item.getString("name"),
                        (Document) item.get("options"));
                subscriber.onNext(resource);
            }
        }
        subscriber.onComplete();
    }
}
