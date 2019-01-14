package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * File: OplogTimestampReader
 * Author: Shyam Arjarapu
 * Date: 1/13/19 7:20 PM
 * Description:
 *
 * A class to help read the latest timestamp from oplog.rs
 *
 */
public class OplogTimestampReader extends ReadOnlyDataTracker {
    final static Logger logger = LoggerFactory.getLogger(OplogTimestampReader.class);

    /**
     * @param client a MongoDB client object to work with collections
     * @param resource a resource representing the collection in a database
     * @param reader a string representation of the current reader / migrator name
     */
    public OplogTimestampReader(MongoClient client, Resource resource, String reader) {
        super(client, resource, reader, "ts");
    }

    /**
     * @param reader a string representation of the current reader / migrator name
     * @param client a MongoDB client object to work with collections
     */
    public OplogTimestampReader(String reader, MongoClient client) {
        this(client, new Resource("local", "oplog.rs"), reader);
    }

    /**
     * Get's the document representing the find query for collection
     *
     * @return a document representing the filter clause to find
     * the tracking information for the collection
     */
    @Override
    protected Document getQueryDocument() {
        return new Document();
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
