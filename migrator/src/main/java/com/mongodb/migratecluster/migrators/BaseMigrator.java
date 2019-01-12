package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;

/**
 * File: BaseMigrator
 * Author: Shyam Arjarapu
 * Date: 1/12/19 12:36 AM
 * Description:
 *
 * An abstract class to represent a migrator which
 * helps migrate entities from source to target by
 * implementing a specific process
 */
public abstract class BaseMigrator {

    protected ApplicationOptions options;

    protected BaseMigrator(ApplicationOptions options) {
        this.options = options;
    }

    /**
     * Get's the Mongo Client pointing to the custom cluster
     *
     * @param cluster a string representing a mongodb servers
     * @return  a MongoClient object pointing to specific cluster
     */
    private MongoClient getMongoClient(String cluster) {
        String connectionString = String.format("mongodb://%s", cluster);
        MongoClientURI uri = new MongoClientURI(connectionString);
        return new MongoClient(uri);
    }

    /**
     * Get's the Mongo Client pointing to the source cluster
     *
     * @return a MongoClient object pointing to source
     */
    protected MongoClient getSourceClient() {
        return getMongoClient(this.options.getSourceCluster());
    }

    /**
     * Get's the Mongo Client pointing to the target cluster
     *
     * @return a MongoClient object pointing to target
     */
    protected MongoClient getTargetClient() {
        return getMongoClient(this.options.getTargetCluster());
    }

    /**
     * Get's the Mongo Client pointing to the oplog cluster
     *
     * @return a MongoClient object pointing to oplog store
     */
    protected MongoClient getOplogClient() {
        return getMongoClient(this.options.getOplogStore());
    }

    /**
     * Checks if the given options are valid
     *
     * @return a boolean representing if the given options are valid
     */
    protected boolean isValidOptions() {
        if ((this.options.getSourceCluster().equals("")) ||
            (this.options.getTargetCluster().equals("")) ||
            (this.options.getOplogStore().equals(""))) {
            return false;
        }
        return true;
    }

    /**
     *
     * A process method that implements actual migration
     * of entities from source to target.
     *
     * @throws AppException
     * @see AppException
     */
    public abstract void process() throws AppException;

    /**
     * A method that is invoked before the actual migration process
     */
    public abstract void preprocess();
}
