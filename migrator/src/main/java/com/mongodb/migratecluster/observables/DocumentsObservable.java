package com.mongodb.migratecluster.observables;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.model.DocumentsBatch;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.in;

/**
 * File: DocumentsObservable
 * Author: shyam.arjarapu
 * Date: 4/17/17 11:39 PM
 * Description:
 */
public class DocumentsObservable extends Observable<DocumentsBatch> {
    private final Resource resource;
    private final int batchId;
    private final Object[] ids;
    private final MongoCollection<Document> collection;

    private final static Logger logger = LoggerFactory.getLogger(DocumentsObservable.class);

    public DocumentsObservable(MongoCollection<Document> collection, Resource resource, int batchId, Object[] ids) {
        this.resource = resource;
        this.batchId = batchId;
        this.ids = ids;
        this.collection = collection;
    }

    @Override
    protected void subscribeActual(Observer<? super DocumentsBatch> observer) {
        List<Document> documents = getDocumentsFromDB();

        String message = String.format("read %s full documents based on given _id's. ", documents.size());
        //TODO: Remove below line
        message = String.format("read %s full documents based on given _id's. ", String.join(",",documents.stream().map(x -> x.get("_id").toString()).toArray(String[]::new)));
        logger.info(message);

        DocumentsBatch batch = new DocumentsBatch(resource, batchId, documents);
        observer.onNext(batch);
        observer.onComplete();
    }

    private List<Document> getDocumentsFromDB() {
        List<Document> docs = new ArrayList<>();
        Bson filter = in("_id", this.ids);
        FindIterable<Document> documents = this.collection.find(filter);
        // find the full documents for given set of _id's
        for (Document item : documents) {
            docs.add(item);
        }
        return docs;
    }
}
