package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;

/**
 * File: migrator
 * Author: shyamarjarapu
 * Date: 6/5/17 11:36 AM
 * Description:
 */
public abstract class BaseMigrator {
    protected ApplicationOptions options;

    public BaseMigrator(ApplicationOptions options) {
        this.options = options;
    }

    private MongoClient getMongoClient(String cluster) {
        String connectionString = String.format("mongodb://%s", cluster);
        MongoClientURI uri = new MongoClientURI(connectionString);
        return new MongoClient(uri);
    }

    protected MongoClient getSourceMongoClient() {
        return getMongoClient(this.options.getSourceCluster());
    }

    protected MongoClient getTargetMongoClient() {
        return getMongoClient(this.options.getTargetCluster());
    }

    protected MongoClient getOplogStoreMongoClient() {
        return getMongoClient(this.options.getOplogStore());
    }

    protected boolean isValidOptions() {
        if ((this.options.getSourceCluster().equals("")) ||
            (this.options.getTargetCluster().equals("")) ||
            (this.options.getOplogStore().equals(""))) {
            return false;
        }
        return true;
    }

    public abstract void process() throws AppException;
}
