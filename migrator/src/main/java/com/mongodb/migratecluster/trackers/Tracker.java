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
public abstract class Tracker extends ReadOnlyTracker implements WritableTracker {

    /**
     * @param resource a resource representing the collection in a database
     */
    protected Tracker(Resource resource) {
        super(resource);
    }

}
