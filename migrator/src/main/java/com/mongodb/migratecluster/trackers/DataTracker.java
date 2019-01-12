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

public abstract class DataTracker extends Tracker {
    protected final String databaseName;
    protected final String collectionName;
    protected final MongoClient client;
    protected final String reader;
    protected final String trackerKey;

    public DataTracker(Resource resource, String databaseName, String collectionName,
                       MongoClient client, String reader, String trackerKey) {
        super(resource);
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.client = client;
        this.reader = reader;
        this.trackerKey = trackerKey;
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
        Document operation = new Document("operation", "find");
        operation.append("query", query);

        MongoCollection<Document> collection = MongoDBHelper.getCollection(client, databaseName, collectionName);
        try {
            return MongoDBHelper.performOperationWithRetry(() -> {
                FindIterable<Document> documents = applyQueryModifiers(collection.find(query)).limit(1);
                return documents.first();
            }, operation);
        } catch (AppException e) {
            CollectionDataTracker.logger.error("Error while finding the latest document for resource {}. Error {}",
                    resource, e.getMessage());
            return null;
        }
    }

    /**
     * @param iterable an iterable which you further want to apply modifiers on
     * @return iterable
     * @see FindIterable<Document>
     */
    protected FindIterable<Document> applyQueryModifiers(FindIterable<Document> iterable) {
        return iterable;
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
            CollectionDataTracker.logger.error("Error while updating latest document {} for resource {}. Error {}",
                    document, resource, e.getMessage());
        }
    }

    /**
     * Get's the document representing the find query for collection
     *
     * @return a document representing the filter clause to find
     * the tracking information for the collection
     */
    protected abstract Document getQueryDocument();

    /**
     * Get's the document representing the update command
     *
     * @param latestDocument a document holding the _id of latest document for current resource
     * @return the document representation of the update $set
     * @see Document
     */
    protected abstract Document getUpdateDocument(Document latestDocument);
}
