package com.mongodb.migratecluster.observables;

import com.mongodb.AggregationOptions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import com.mongodb.migratecluster.commandline.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
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
        Bson projection = new BsonDocument("_id", new BsonInt32(1));
        FindIterable<Document> documents =
                this.collection
                    .find()
                    .projection(projection)
                    .sort(sortCriteria);
        for (Document item : documents) {
            if (!item.isEmpty()) {
                observer.onNext(new ResourceDocument(this.resource, item));
            }
        }
        observer.onComplete();
    }
}
