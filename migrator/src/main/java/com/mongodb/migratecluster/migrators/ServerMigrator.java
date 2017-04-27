package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.DocumentsObservable;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * File: ServerMigrator
 * Author: shyam.arjarapu
 * Date: 4/14/17 11:41 PM
 * Description:
 */
public class ServerMigrator {
    private final static Logger logger = LoggerFactory.getLogger(ServerMigrator.class);
    private final MongoClient client;
    private List<DatabaseMigrator> migrators;

    public ServerMigrator(MongoClient client, Map<String, List<Resource>> filteredSourceResources) throws AppException {
        this.client = client;
        this.initialize(filteredSourceResources);
    }

    private void initialize(Map<String, List<Resource>> dbResources) throws AppException {
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
                migrators.add(migrator);
            }
        }
    }

    public List<DatabaseMigrator> getDatabaseMigrators() {
        return migrators;
    }
}
