package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.utils.ListUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.mongodb.migratecluster.utils.ListUtils.select;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class DatabaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class);
    private final MongoClient client;
    private final String database;
    private final List<Resource> resources;

    public DatabaseMigrator(MongoClient client, String database, List<Resource> resources) {
        this.client = client;
        this.database = database;
        this.resources = resources;
    }

    public String getDatabase() {
        return database;
    }


    public void migrate(ApplicationOptions appOptions) throws AppException {
        if (this.resources.size() == 0) {
            String message = "looks like no collections found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        List<CollectionMigrator> migrators = select(this.resources, r -> new CollectionMigrator(client, r));
        for (CollectionMigrator migrator : migrators) {
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
