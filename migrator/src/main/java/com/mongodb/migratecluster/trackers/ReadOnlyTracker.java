package com.mongodb.migratecluster.trackers;

import com.mongodb.migratecluster.model.Resource;
import org.bson.Document;

/**
 *
 * File: ReadOnlyTracker
 * Author: Shyam Arjarapu
 * Date: 1/13/19 7:10 AM
 * Description:
 *
 * An abstract class representing a readonly tracker that helps
 * you track the latest document for a resource
 *
 */
public abstract class ReadOnlyTracker {
    protected final Resource resource;

    public ReadOnlyTracker(Resource resource) {
        this.resource = resource;
    }

    /**
     * Get's the latest document associated with the resource
     *
     * @return document a document representing the most recently processed
     * @see Document
     */
    public abstract Document getLatestDocument();
}
