package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.migratecluster.utils.ListUtils.*;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class ServerMigrator {
    final static Logger logger = LoggerFactory.getLogger(ServerMigrator.class);
    private final MongoClient client;

    public ServerMigrator(MongoClient client) {
        this.client = client;
    }

    public void migrate(ApplicationOptions appOptions, Map<String, List<Resource>> dbResources) throws AppException {
        if (dbResources.size() == 0) {
            String message = "looks like no databases found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        List<DatabaseMigrator> migrators = new ArrayList<>();

        dbResources.keySet().forEach(db -> {
            List<Resource> resources = dbResources.get(db);
            if (resources != null && resources.size() > 0) {
                migrators.add(new DatabaseMigrator(client, db, resources));
            }
        });

        for (DatabaseMigrator migrator : migrators) {
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
