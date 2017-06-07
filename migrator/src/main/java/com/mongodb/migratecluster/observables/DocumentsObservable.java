package com.mongodb.migratecluster.observables;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.in;

/**
 * File: DocumentsObservable
 * Author: shyam.arjarapu
 * Date: 4/17/17 11:39 PM
 * Description:
 */
public class DocumentsObservable extends Observable<List<ResourceDocument>> {
    private final Resource resource;

    private final Object[] ids;
    private final MongoCollection<Document> collection;

    private final static Logger logger = LoggerFactory.getLogger(DocumentsObservable.class);

    public DocumentsObservable(MongoCollection<Document> collection, Resource resource, Object[] ids) {
        this.resource = resource;
        this.ids = ids;
        this.collection = collection;
    }

    @Override
    protected void subscribeActual(Observer<? super List<ResourceDocument>> observer) {
        List<Document> documents = getDocumentsFromDB();
        List<ResourceDocument> resourceDocuments = createResourceDocumentsFrom(documents);

        String message = String.format("read %s full documents based on given _id's. ", documents.size());
        logger.info(message);

        observer.onNext(resourceDocuments);
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

    private List<ResourceDocument> createResourceDocumentsFrom(List<Document> docs) {
        // create resource documents from bson.document
        return docs.stream()
                .map(d -> new ResourceDocument(this.resource, d))
                .collect(Collectors.toList());
    }
}
