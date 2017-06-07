package com.mongodb.migratecluster.oplog;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.migrators.DataMigrator;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * File: OplogGapWatcher
 * Author: shyam.arjarapu
 * Date: 6/7/17 1:03 PM
 * Description: Get the latest oplog time from source replica set,
 * oplogStore replica set and publishes the time gap between them
 */
public class OplogGapWatcher  extends Observable<OplogGap> {

    private final MongoClient oplogStoreClient;
    private final MongoClient sourceClient;
    private final String name;

    final static Logger logger = LoggerFactory.getLogger(DataMigrator.class);

    public OplogGapWatcher(MongoClient sourceClient, MongoClient oplogStoreClient, String name) {
        this.sourceClient = sourceClient;
        this.oplogStoreClient = oplogStoreClient;
        this.name = name;
    }

    @Override
    protected void subscribeActual(Observer<? super OplogGap> observer) {
        Document noOpFilter = new Document("op", new Document("$ne", "n"));
        Document targetFilter = getTargetFilter(noOpFilter);

        // wait for 5 seconds and then start the interval of 5 seconds
        Observable<Long> observable = Observable
                .timer(5, TimeUnit.SECONDS)
                .interval(5, TimeUnit.SECONDS)
                .observeOn(Schedulers.computation())
                .subscribeOn(Schedulers.newThread());

        observable.subscribe(time -> {
                BsonTimestamp sourceTimestamp = getLatestOplogTimestamp(
                        this.sourceClient,
                        noOpFilter,
                        "local",
                        "oplog.rs");
                BsonTimestamp oplogStoreTimestamp = getLatestOplogTimestamp(
                        this.oplogStoreClient,
                        targetFilter,
                        "migrate-mongo",
                        "oplog.tracker");

                OplogGap result = new OplogGap(sourceTimestamp, oplogStoreTimestamp);
                observer.onNext(result);
            });
        observable.blockingLast();
    }

    private Document getTargetFilter(Document noOpFilter) {
        Document readerFilter = new Document("reader", this.name);
        List<Document> filters = new ArrayList<>();
        filters.add(noOpFilter);
        filters.add(readerFilter);
        return new Document("$and", filters);
    }

    public static BsonTimestamp getLatestOplogTimestamp(MongoClient client,
                    Document filter, String databaseName, String collectionName) throws AppException {
        MongoCollection<Document> collection = MongoDBHelper.getCollection(
                client, databaseName, collectionName);

        BsonTimestamp timestamp = MongoDBHelper.performMongoOperationWithRetry(() -> {
            BsonTimestamp ts = null;
            MongoCursor<Document> cursor =
                collection
                    .find(filter)
                    .sort(new Document("$natural", -1))
                    .projection(new Document("ts", 1))
                    .limit(1)
                    .iterator();

            if (cursor.hasNext()) {
                ts = cursor.next().get("ts", BsonTimestamp.class);
            }
            return ts;
        }, new Document());

        return timestamp;
    }
}
