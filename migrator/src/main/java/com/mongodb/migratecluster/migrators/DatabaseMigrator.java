package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.DocumentObservable;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * File: DatabaseMigrator
 * Author: shyam.arjarapu
 * Date: 4/14/17 11:46 PM
 * Description:
 */
public class DatabaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class);
    private final MongoClient client;
    private final String database;
    private final List<Resource> resources;
    private List<CollectionMigrator> migrators;

    public DatabaseMigrator(MongoClient client, String database, List<Resource> resources) throws AppException {
        this.client = client;
        this.database = database;
        this.resources = resources;
        this.initialize();
    }

    public String getDatabase() {
        return database;
    }

    public Observable<DocumentObservable> getObservable() {
        return Observable
                .fromIterable(this.migrators)
                .map(d -> d.getObservable());
    }

    private void initialize() throws AppException {
        if (this.resources.size() == 0) {
            String message = "looks like no collections found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        this.migrators = new ArrayList<>();
        for (Resource resource : this.resources) {
            CollectionMigrator migrator = new CollectionMigrator(client, resource);
            this.migrators.add(migrator);
        }
    }
}
