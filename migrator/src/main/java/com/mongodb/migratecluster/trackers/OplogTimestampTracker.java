package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * File: OplogTimestampTracker
 * Author: Shyam Arjarapu
 * Date: 1/12/19 9:45 AM
 * Description:
 *
 * A class representing a tracker for oplog resource.
 * It helps you track the latest timestamp on migrator
 *
 */
public class OplogTimestampTracker extends WritableDataTracker {
    final static Logger logger = LoggerFactory.getLogger(OplogTimestampTracker.class);
    protected final String reader;
    protected final String trackerKey;

    /**
     * @param client a MongoDB client object to work with collections
     * @param resource a resource representing the collection in a database
     * @param reader a string representation of the current reader / migrator name
     */
    public OplogTimestampTracker(MongoClient client, Resource resource, String reader) {
        super(client, resource);
        this.reader = reader;
        this.trackerKey = "ts";
    }


    /**
     * Get's the document representing the find query for collection
     *
     * @return a document representing the filter clause to find
     * the tracking information for the collection
     */
    @Override
    protected Document getQueryDocument() {
        return new Document("reader", reader);
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
        return new Document("$set", new Document(trackerKey, latestDocument.get("ts")));
    }

    /**
     * @param iterable an iterable which you further want to apply modifiers on
     * @return iterable
     * @see FindIterable<Document>
     */
    @Override
    protected FindIterable<Document> applyQueryModifiers(FindIterable<Document> iterable) {
        return iterable.sort(new Document("$natural", -1));
    }
}
