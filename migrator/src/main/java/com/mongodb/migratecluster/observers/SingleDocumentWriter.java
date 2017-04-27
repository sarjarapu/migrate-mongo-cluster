package com.mongodb.migratecluster.observers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.ResourceDocument;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: SingleDocumentWriter
 * Author: shyam.arjarapu
 * Date: 4/17/17 11:53 PM
 * Description:
 */
public class SingleDocumentWriter extends BaseDocumentWriter implements Observer<ResourceDocument> {
    private final static Logger logger = LoggerFactory.getLogger(SingleDocumentWriter.class);

    public SingleDocumentWriter(MongoClient client) {
        super(client);
    }

    @Override
    public void onSubscribe(Disposable disposable) {

    }

    @Override
    public void onNext(ResourceDocument resourceDocument) {
        writeDocument(resourceDocument);
        String message = String.format(" insert document into %s {_id: \"%s\"}",
                resourceDocument.getResource().getNamespace(),
                resourceDocument.getDocument().get("_id"));
        logger.debug(message);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }

/*    private void createCollectionIfRequired(ResourceDocument resourceDocument) {
        Resource resource = resourceDocument.getResource();
        String dbName = resource.getDatabase();

        MongoDatabase database = getMongoDatabase(dbName);
        if (database != null) {
            Document options = resource.getCollectionOptions();
            if (options.isEmpty()) {
                database.createCollection(resource.getCollection());
            }
            else {
                CreateCollectionOptions collectionOptions = getCreateCollectionOptions(options);
                database.createCollection(resource.getCollection(), collectionOptions);
            }
        }
    }*/

    private void writeDocument(ResourceDocument resourceDocument) {
        Resource resource = resourceDocument.getResource();
        MongoCollection<Document> collection =
                getMongoCollection(
                    resource.getNamespace(),
                    resource.getDatabase(),
                    resource.getCollection());
        collection.insertOne(resourceDocument.getDocument());
    }


}
