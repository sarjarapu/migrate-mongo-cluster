package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.DocumentObservable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: CollectionMigrator
 * Author: shyam.arjarapu
 * Date: 4/14/17 11:46 PM
 * Description:
 */
public class CollectionMigrator {
    final static Logger logger = LoggerFactory.getLogger(CollectionMigrator.class);
    private final MongoClient client;
    private final Resource resource;
    private DocumentObservable documents;

    public CollectionMigrator(MongoClient client, Resource resource) throws AppException {
        this.client = client;
        this.resource = resource;
        initialize();
    }

    public String getNamespace() {
        return this.resource.getNamespace();
    }

    public DocumentObservable getObservable() {
        return documents;
    }

    private void initialize() throws AppException {
        MongoCollection<Document> collection = IteratorHelper.getMongoCollection(this.client, this.resource);

        if (collection == null) {
            String message = "looks like no collections found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        this.documents = new DocumentObservable(this.resource, collection);
    }
}
