package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.DocumentObservable;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class DatabaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class);
    private final MongoClient client;
    private final String database;
    private final List<Resource> resources;
    private List<CollectionMigrator> migrators;

    public DatabaseMigrator(MongoClient client, String database, List<Resource> resources) {
        this.client = client;
        this.database = database;
        this.resources = resources;
    }

    public String getDatabase() {
        return database;
    }

    public Observable<DocumentObservable> getObservable() {
        return Observable
                .fromIterable(this.migrators)
                .map(d -> d.getObservable());
    }

    public void initialize() throws AppException {
        if (this.resources.size() == 0) {
            String message = "looks like no collections found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        this.migrators = new ArrayList<>();
        for (Resource resource : this.resources) {
            CollectionMigrator migrator = new CollectionMigrator(client, resource);
            migrator.initialize();
            this.migrators.add(migrator);
        }
    }

    public void migrate(ApplicationOptions appOptions) throws AppException {
        for (CollectionMigrator migrator : this.migrators) {
            try {
                migrator.migrate(appOptions);
            } catch (AppException e) {
                String message = String.format("error while migrating collection: %s", migrator.getNamespace());
                logger.error(message);
                throw new AppException(message, e);
            }
        }
    }
}
