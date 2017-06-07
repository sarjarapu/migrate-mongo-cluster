package com.mongodb.migratecluster.observables;


import com.mongodb.MongoClient;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCursor;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: DatabaseFlowable
 * Author: shyam.arjarapu
 * Date: 4/26/17 6:05 PM
 * Description:
 */
public class DatabaseFlowable extends Observable<Document> {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseFlowable.class);
    private final MongoClient client;

    public DatabaseFlowable(MongoClient client) {
        this.client = client;
    }

    @Override
    protected void subscribeActual(Observer<? super Document> subscriber) {
        ListDatabasesIterable<Document> cursor = client.listDatabases();
        MongoCursor<Document> iterator = cursor.iterator();

        while(iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                String message = String.format("found database name: %s, sizeOnDisk: %s",
                        item.getString("name"), item.get("sizeOnDisk"));
                //logger.info(message);
                //logger.debug(item.toJson());
                subscriber.onNext(item);
            }
        }
        subscriber.onComplete();
    }
}
