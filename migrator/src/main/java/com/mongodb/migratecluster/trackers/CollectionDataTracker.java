package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * File: CollectionDataTracker
 * Author: Shyam Arjarapu
 * Date: 1/11/19 10:00 PM
 * Description:
 *
 * A class representing a tracker for collection resource.
 * It helps you track the latest document in a collection
 *
 */
public class CollectionDataTracker extends Tracker {
    final static Logger logger = LoggerFactory.getLogger(CollectionDataTracker.class);

    private final String databaseName = "migrate-mongo";
    private final String collectionName = "collections";

    private final MongoClient client;
    private final String reader;

    /**
     * @param client
     * @param resource a resource representing the collection in a database
     */
    public CollectionDataTracker(String reader, MongoClient client, Resource resource) {
        super(resource);
        this.reader = reader;
        this.client = client;
    }

    /**
     * Get's the latest document associated with the resource
     *
     * @return document a document representing the most recently processed
     * @see Document
     */
    @Override
    public Document getLatestDocument() {
        Document query = getQueryDocument();
        Document operation = new Document("operation", "update");
        operation.append("query", query);

        MongoCollection<Document> collection = MongoDBHelper.getCollection(client, databaseName, collectionName);
        try {
            MongoDBHelper.performOperationWithRetry(() -> {
                FindIterable<Document> documents = collection.find(query).limit(1);
                return documents.first();
            }, operation);
        } catch (AppException e) {
            logger.error("Error while finding the latest document for resource {}. Error {}",
                    resource, e.getMessage());
        }
        return null;
    }

    /**
     * Saves the latest document into the trackers database for given resource
     *
     * @param document a document representing the most recently processed
     * @see Document
     */
    @Override
    public void updateLatestDocument(Document document) {
        Document query = getQueryDocument();
        Document update = getUpdateDocument(document);

        Document operation = new Document("operation", "update");
        operation.append("query", query);
        operation.append("update", update);

        MongoCollection<Document> collection = MongoDBHelper.getCollection(client, databaseName, collectionName);
        try {
            MongoDBHelper.performOperationWithRetry(() -> {
                UpdateOptions options = new UpdateOptions().upsert(true);
                UpdateResult result = collection.updateOne(query, update, options);
                return result;
            }, operation);
        } catch (AppException e) {
            logger.error("Error while updating latest document {} for resource {}. Error {}",
                    document, resource, e.getMessage());
        }
    }

    /**
     * Get's the document representing the find query for collection
     *
     * @return a document representing the filter clause to find
     * the tracking information for the collection
     */
    private Document getQueryDocument() {
        Document readerFilter = new Document("reader", reader);
        Document databaseFilter = new Document("database", resource.getDatabase());
        Document collectionFilter = new Document("collection", resource.getCollection());

        List<Document> filters = new ArrayList<>();
        filters.add(readerFilter);
        filters.add(databaseFilter);
        filters.add(collectionFilter);

        return new Document("$and", filters);
    }

    /**
     * Get's the document representing the update command
     *
     * @param latestDocument a document holding the _id of latest document for current resource
     * @return the document representation of the update $set
     * @see Document
     */
    private Document getUpdateDocument(Document latestDocument) {
        return new Document("$set", new Document("latest_id", latestDocument.get("_id")));
    }
}
