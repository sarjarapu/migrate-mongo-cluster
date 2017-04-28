package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.migrators.DataMigrator;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * File: DocumentReader
 * Author: shyam.arjarapu
 * Date: 4/26/17 4:48 AM
 * Description:
 */
public class DocumentReader  extends Observable<List<ResourceDocument>> {
    final static Logger logger = LoggerFactory.getLogger(DocumentReader.class);
    private final Resource resource;
    private  MongoClient client;
    private  MongoCollection<Document> collection;

    public DocumentReader(MongoClient client, Resource resource) {
        this.resource = resource;
        this.collection = client.getDatabase(resource.getDatabase()).getCollection(resource.getCollection());
    }

    // TODO: Remove this code
    public DocumentReader(MongoCollection<Document> collection, Resource resource) {
        this.collection = collection;
        this.resource = resource;
    }

    @Override
    protected void subscribeActual(Observer<? super List<ResourceDocument>> observer) {
        Observable<Object> observable = getDocumentIdsObservable(collection);
        AtomicInteger docsCount = new AtomicInteger(0);
        // ideally return this
        observable
                .buffer(1000)
                .flatMap(new Function<List<Object>, Observable<List<ResourceDocument>>>() {
                    @Override
                    public Observable<List<ResourceDocument>> apply(List<Object> ids) throws Exception {
                        return new DocumentsObservable(collection, getResource(), ids.toArray())
                               .subscribeOn(Schedulers.io());
                    }
                })
                .forEach(k -> {
                    if (k.size() > 0) {
                        // BUG; Looks like this line is getting executed  twice  for some reason
                        logger.info("**** DocumentReader. Got {} documents; Total read so far: {} ", k.size(), docsCount.addAndGet(k.size()));
                    }
                    //logger.info("**** DocumentReader => I think this is where the error is coming in from k: {}", k);
                    observer.onNext(k);
                });
        // NOTE: by not blocking here, there is possibility of missing last set in the buffer
        observable.blockingLast();
        observer.onComplete();
        logger.info("**** DocumentReader => OnComplete. Total Documents Read: {}", docsCount);
    }

    private Observable<Object> getDocumentIdsObservable(MongoCollection<Document> collection) {
        return new Observable<Object>() {
            @Override
            protected void subscribeActual(Observer<? super Object> observer) {
                FindIterable<Document> documents = collection
                        .find()
                        .projection(BsonDocument.parse("{_id: 1}"))
                        .sort(BsonDocument.parse("{$natural: 1}"))
                        .batchSize(5000);
                for (Document item : documents) {
                    if (!item.isEmpty()) {
                        observer.onNext(item.get("_id"));
                    }
                }
                observer.onComplete();
            }
        };
    }

    public Resource getResource() {
        return resource;
    }
}