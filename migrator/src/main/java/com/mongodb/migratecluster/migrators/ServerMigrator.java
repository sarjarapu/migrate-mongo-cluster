package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.CollectionObservable;
import com.mongodb.migratecluster.observables.DocumentObservable;
import io.reactivex.Observable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class ServerMigrator {
    final static Logger logger = LoggerFactory.getLogger(ServerMigrator.class);
    private final MongoClient client;
    private List<DatabaseMigrator> migrators;

    public ServerMigrator(MongoClient client) {
        this.client = client;
    }

    public void initialize(Map<String, List<Resource>> dbResources) throws AppException {
        if (dbResources.size() == 0) {
            String message = "looks like no databases found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        this.migrators = new ArrayList<>();
        for (String db : dbResources.keySet()) {
            List<Resource> resources = dbResources.get(db);
            if (resources != null && resources.size() > 0) {
                DatabaseMigrator migrator = new DatabaseMigrator(client, db, resources);
                migrator.initialize();
                migrators.add(migrator);
            }
        }
    }

    public Observable<DocumentObservable> getObservable() {
        return Observable
                .fromIterable(this.migrators)
                .flatMap(m -> m.getObservable());
    }

    public void migrate(ApplicationOptions appOptions) throws AppException {
        for (DatabaseMigrator migrator : this.migrators) {
            try {
                migrator.migrate(appOptions);
            } catch (AppException e) {
                String message = String.format("error while migrating database: %s", migrator.getDatabase());
                logger.error(message);
                throw new AppException(message, e);
            }
        }
    }

}
