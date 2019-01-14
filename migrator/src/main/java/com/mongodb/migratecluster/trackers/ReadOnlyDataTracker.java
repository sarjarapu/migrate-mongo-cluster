package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;

public abstract class ReadOnlyDataTracker extends ReadOnlyTracker {
    protected final MongoClient client;

    /**
     * @param client a MongoDB client object to work with collections
     * @param resource a resource representing the collection in a database
     */
    public ReadOnlyDataTracker(MongoClient client, Resource resource) {
        super(resource);
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
        Document operation = new Document("operation", "find");
        operation.append("query", query);

        MongoCollection<Document> collection = MongoDBHelper.getCollection(client,
                resource.getDatabase(), resource.getCollection());
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
     * Get's the document representing the find query for collection
     *
     * @return a document representing the filter clause to find
     * the tracking information for the collection
     */
    protected abstract Document getQueryDocument();
}
