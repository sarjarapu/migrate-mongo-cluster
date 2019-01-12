package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
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
public class CollectionDataTracker extends DataTracker {
    final static Logger logger = LoggerFactory.getLogger(CollectionDataTracker.class);

    /**
     * @param reader a string representation of the current reader / migrator name
     * @param client a MongoDB client object to work with collections
     * @param resource a resource representing the collection in a database
     */
    public CollectionDataTracker(String reader, MongoClient client, Resource resource) {
        super(resource, "migrate-mongo", "collections",
                client, reader, "latest_id");
    }

    /**
     * Get's the document representing the find query for collection
     *
     * @return a document representing the filter clause to find
     * the tracking information for the collection
     */
    @Override
    protected Document getQueryDocument() {
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
    @Override
    protected Document getUpdateDocument(Document latestDocument) {
        return new Document("$set", new Document(trackerKey, latestDocument.get("_id")));
    }
}
