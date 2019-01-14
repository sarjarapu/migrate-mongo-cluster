package com.mongodb.migratecluster.trackers;

import org.bson.Document;

public interface WritableTracker {
    /**
     * Saves the latest document into the trackers database for given resource
     *
     * @param document a document representing the most recently processed
     * @see Document
     */
    void updateLatestDocument(Document document);
}
