package com.mongodb.migratecluster.observables;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import com.mongodb.migratecluster.commandline.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * File: DocumentObservable
 * Author: shyam.arjarapu
 * Date: 4/17/17 11:39 PM
 * Description:
 */
public class DocumentObservable extends Observable<ResourceDocument> {
    private final Resource resource;
    private MongoCollection<Document> collection;

    public DocumentObservable(Resource resource, MongoCollection<Document> collection) {
        this.collection = collection;
        this.resource = resource;
    }

    @Override
    protected void subscribeActual(Observer<? super ResourceDocument> observer) {
        Bson sortCriteria = Sorts.ascending("_id");
        for (Document item : this.collection.find().sort(sortCriteria)) {
            if (!item.isEmpty()) {
                observer.onNext(new ResourceDocument(this.resource, item));
            }
        }
        observer.onComplete();
    }
}
