package com.mongodb.migratecluster.trackers;

import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;

/**
 *
 * File: Tracker
 * Author: Shyam Arjarapu
 * Date: 1/12/19 9:10 AM
 * Description:
 *
 * An abstract class representing a tracker that helps
 * you track / update the latest document for a resource
 *
 */
public abstract class Tracker {

    protected final Resource resource;

    /**
     * @param resource a resource representing the collection in a database
     */
    protected Tracker(Resource resource) {
        this.resource = resource;
    }

    /**
     * Get's the latest document associated with the resource
     *
     * @return document a document representing the most recently processed
     * @see Document
     */
    public abstract Document getLatestDocument();

    /**
     * Saves the latest document into the trackers database for given resource
     *
     * @param document a document representing the most recently processed
     * @see Document
     */
    public abstract void updateLatestDocument(Document document);
}
