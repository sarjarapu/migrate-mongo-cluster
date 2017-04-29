package com.mongodb.migratecluster.observables;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: DocumentIdReader
 * Author: shyam.arjarapu
 * Date: 4/28/17 6:29 AM
 * Description:
 */
public class DocumentIdReader extends Observable<Object> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentIdReader.class);
    private final MongoCollection<Document> collection;
    private final Resource resource;

    public DocumentIdReader(MongoCollection<Document> collection, Resource resource) {
        this.collection = collection;
        this.resource = resource;
    }

    @Override
    protected void subscribeActual(Observer<? super Object> observer) {
        // TODO: Find the id and continue from where you left off
        FindIterable<Document> documents = collection
                .find()
                .projection(BsonDocument.parse("{_id: 1}"))
                .sort(BsonDocument.parse("{$natural: 1}"))
                .batchSize(5000);

        for (Document item : documents) {
            if (!item.isEmpty()) {
                // TODO: Turn on the throttling here
                logger.info("reading _id 1 by 1; found _id: {}", item.get("_id"));
                observer.onNext(item.get("_id"));
            }
        }
        observer.onComplete();
    }
}
