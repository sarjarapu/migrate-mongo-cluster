package com.mongodb.migratecluster.trackers;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.migratecluster.migrators.OplogMigrator;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.oplog.OplogGapWatcher;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
    protected final String reader;
    protected final String trackerKey;

    /**
     * @param client a MongoDB client object to work with collections
     * @param resource a resource representing the collection in a database
     * @param reader a string representation of the current reader / migrator name
     */
    public OplogTimestampReader(MongoClient client, Resource resource, String reader) {
        super(client, resource);
        this.reader = reader;
        this.trackerKey = "ts";
    }

    /**
     * @param client a MongoDB client object to work with collections
     * @param reader a string representation of the current reader / migrator name
     */
    public OplogTimestampReader(MongoClient client, String reader) {
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
        Document filter = getDefaultIgnoreFilter();
        iterable.filter(filter);
        return iterable.sort(new Document("$natural", -1));
    }

    private Document getDefaultIgnoreFilter() {
        List<Document> filters = new ArrayList<>();
        Document noOpFilter = new Document("op", new Document("$ne", "n"));
        List<String> systemNamespaces = Arrays.asList("config.$cmd", "admin.$cmd", "admin.system.keys", "config.system.sessions");
        Document systemFilter = new Document("ns", new Document("$nin", systemNamespaces));

        filters.add(noOpFilter);
        filters.add(systemFilter);
        return new Document("$and", filters);
    }
}
