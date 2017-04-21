package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.observers.BulkDocumentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * File: DataMigrator
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:45 PM
 * Description:
 */
public class DataMigrator {
    final static Logger logger = LoggerFactory.getLogger(DataMigrator.class);
    private ApplicationOptions appOptions;

    public DataMigrator(ApplicationOptions appOptions) {
        this.appOptions = appOptions;
    }

    private boolean isValidOptions() {
        // on appOptions source, target, oplog must all be present
        if (
                (this.appOptions.getSourceCluster() == "") ||
                (this.appOptions.getTargetCluster() == "") ||
                (this.appOptions.getOplogStore() == "")
            ) {
            // invalid input
            return false;
        }
        return true;
    }

    public void process() throws AppException {
        // check if the appOptions are valid
        if (!this.isValidOptions()) {
            String message = String.format("invalid input args for sourceCluster, targetCluster and oplog. \ngiven: %s", this.appOptions.toString());
            throw new AppException(message);
        }

        // loop through source and copy to target
        readSourceClusterDatabases();
    }

    private void readSourceClusterDatabases() throws AppException {
        MongoClient sourceClient = getSourceMongoClient();
        MongoClient targetClient = getTargetMongoClient();
        Map<String, List<Resource>> sourceResources = IteratorHelper.getSourceResources(sourceClient);
        Map<String, List<Resource>> filteredSourceResources = getFilteredResources(sourceResources);


        try {
            Date startDateTime = new Date();
            logger.info(" started processing at {}", startDateTime);
            ServerMigrator serverMigrator = new ServerMigrator(sourceClient, filteredSourceResources);
            BulkDocumentWriter bulkDocumentWriter = new BulkDocumentWriter(targetClient);

            serverMigrator
                .getDatabaseMigrators()
                .forEach(dm -> {
                    dm.getCollectionMigrators()
                        .forEach(cm -> {
                            cm.getObservable()
                                .buffer(500)
                                .subscribe(bulkDocumentWriter);
                        });
                });
            /*
            // previously working
            // single threaded bulkDocumentWriter
            serverMigrator
                    .getObservable()
                    .flatMap(d -> d)
                    .buffer(500)
                    ////.
                    //.subscribeOn(Schedulers.io())
                    //.observeOn(Schedulers.single())
                    .subscribe(bulkDocumentWriter);
            */
            Date endDateTime = new Date();
            logger.info(" completed processing at {}", endDateTime);
            logger.info(" total time to process is {}", TimeUnit.SECONDS.convert(endDateTime.getTime() - startDateTime.getTime(), TimeUnit.MILLISECONDS));
        } catch (AppException e) {
            String message = "error in while processing server migration.";
            logger.error(message, e);
            throw new AppException(message, e);
        }
        sourceClient.close();
    }

    private MongoClient getMongoClient(String cluster) {
        String connectionString = String.format("mongodb://%s", cluster);
        MongoClientURI uri = new MongoClientURI(connectionString);
        return new MongoClient(uri);
    }

    private MongoClient getSourceMongoClient() {
        return getMongoClient(this.appOptions.getSourceCluster());
    }

    private MongoClient getTargetMongoClient() {
        return getMongoClient(this.appOptions.getTargetCluster());
    }

    private Map<String, List<Resource>> getFilteredResources(Map<String, List<Resource>> resources) {
        List<ResourceFilter> blacklist = appOptions.getBlackListFilter();
        Map<String, List<Resource>> filteredResources = new HashMap<>(resources);

        // for all resources in blacklist remove them from filteredResources
        blacklist.forEach(r -> {
            String db = r.getDatabase();
            String coll = r.getCollection();
            if (filteredResources.containsKey(db)) {
                // check if entire database needs to be skipped
                if (r.isEntireDatabase()) {
                    filteredResources.remove(db);
                }
                else {
                    // otherwise just remove the resources by collection name
                    List<Resource> list = filteredResources.get(db);
                    list.removeIf(i -> i.getCollection().equals(coll));
                }
            }
        });

        // remove database if it has any empty resource list in it
        Object[] dbNames = filteredResources.keySet().toArray();
        for (Object db : dbNames) {
            String name = db.toString();
            if (filteredResources.get(name).size() == 0) {
                filteredResources.remove(name);
            }
        }

        return filteredResources;
    }
}
