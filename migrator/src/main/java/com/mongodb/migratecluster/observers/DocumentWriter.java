package com.mongodb.migratecluster.observers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.migrators.IteratorHelper;
import com.mongodb.migratecluster.observables.ResourceDocument;
import com.mongodb.util.JSON;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * File: DocumentWriter
 * Author: shyam.arjarapu
 * Date: 4/17/17 11:53 PM
 * Description:
 */
public class DocumentWriter implements Observer<ResourceDocument> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentWriter.class);

    private final MongoClient client;
    private final Map<String, MongoDatabase> mongoDatabaseMap;
    private final Map<String, MongoCollection<Document>> mongoCollectionMap;

    public DocumentWriter(MongoClient client) {
        this.client = client;
        this.mongoDatabaseMap = new HashMap<>();
        this.mongoCollectionMap = new HashMap<>();
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

    private void createCollectionIfRequired(ResourceDocument resourceDocument) {
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
    }

    private void writeDocument(ResourceDocument resourceDocument) {
        Resource resource = resourceDocument.getResource();
        MongoCollection<Document> collection =
                getMongoCollection(
                    resource.getNamespace(),
                    resource.getDatabase(),
                    resource.getCollection());
        collection.insertOne(resourceDocument.getDocument());
    }

    private MongoDatabase getMongoDatabase(String dbName) {
        // TODO: Concurrent Dictionary ?
        if (mongoDatabaseMap.containsKey(dbName)) {
            return mongoDatabaseMap.get(dbName);
        }

        MongoDatabase database = this.client.getDatabase(dbName);
        mongoDatabaseMap.put(dbName, database);
        return database;
    }


    private MongoCollection<Document> getMongoCollection(String namespace, String databaseName, String collectionName) {
        // TODO: Concurrent Dictionary ?
        if (mongoCollectionMap.containsKey(namespace)) {
            return mongoCollectionMap.get(namespace);
        }

        MongoDatabase database = getMongoDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        mongoCollectionMap.put(namespace, collection);
        return collection;
    }

    private CreateCollectionOptions getCreateCollectionOptions(Document document) {
        CreateCollectionOptions collectionOptions = new CreateCollectionOptions();

        if (document.containsKey("autoIndex")) {
            collectionOptions.autoIndex(document.getBoolean("autoIndex"));
        }
        if (document.containsKey("capped")) {
            collectionOptions.capped(document.getBoolean("capped"));
        }
        if (document.containsKey("maxDocuments")) {
            collectionOptions.maxDocuments(document.getLong("maxDocuments"));
        }
        if (document.containsKey("sizeInBytes")) {
            collectionOptions.sizeInBytes(document.getLong("sizeInBytes"));
        }
        if (document.containsKey("usePowerOf2Sizes")) {
            collectionOptions.usePowerOf2Sizes(document.getBoolean("usePowerOf2Sizes"));
        }

        return collectionOptions;
    }
}
