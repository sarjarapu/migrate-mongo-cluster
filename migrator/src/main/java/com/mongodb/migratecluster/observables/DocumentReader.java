package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.List;

/**
 * File: DocumentReader
 * Author: shyam.arjarapu
 * Date: 4/26/17 4:48 AM
 * Description:
 */
public class DocumentReader  extends Observable<List<ResourceDocument>> {
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

        // ideally return this
        observable
                .buffer(1000)
                .flatMap(new Function<List<Object>, Observable<List<ResourceDocument>>>() {
                    @Override
                    public Observable<List<ResourceDocument>> apply(List<Object> ids) throws Exception {
                        return new DocumentsObservable(collection, getResource(), ids.toArray());
                                //.subscribeOn(Schedulers.io());
                    }
                })
                .forEach(k -> {
                    observer.onNext(k);
                });
        //observable.blockingLast();
        observer.onComplete();
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