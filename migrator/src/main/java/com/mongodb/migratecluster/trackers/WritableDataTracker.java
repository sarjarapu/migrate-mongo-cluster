package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;

public abstract class WritableDataTracker extends ReadOnlyDataTracker implements WritableTracker {

    public WritableDataTracker(MongoClient client, Resource resource) {
        super(client, resource);
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

        MongoCollection<Document> collection = MongoDBHelper.getCollection(client,
                resource.getDatabase(), resource.getCollection());
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
     * Get's the document representing the update command
     *
     * @param latestDocument a document holding the _id of latest document for current resource
     * @return the document representation of the update $set
     * @see Document
     */
    protected abstract Document getUpdateDocument(Document latestDocument);
}
