package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.observables.*;
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
    private ApplicationOptions options;

    public DataMigrator(ApplicationOptions options) {
        this.options = options;
    }

    private boolean isValidOptions() {
        // source, target, oplog
        if (
                (this.options.getSourceCluster().equals("")) ||
                (this.options.getTargetCluster().equals("")) ||
                (this.options.getOplogStore().equals(""))
            ) {
            // invalid input
            return false;
        }
        return true;
    }

    public void process() throws AppException {
        // check if the options are valid
        if (!this.isValidOptions()) {
            String message = String.format("invalid input args for sourceCluster, targetCluster and oplog. \ngiven: %s", this.options.toString());
            throw new AppException(message);
        }

        // loop through source and copy to target
        readSourceClusterDatabases();
    }

    private void readSourceClusterDatabases() throws AppException {
        MongoClient sourceClient = getSourceMongoClient();
        MongoClient targetClient = getTargetMongoClient();
        //Map<String, List<Resource>> sourceResources = MongoDBIteratorHelper.getSourceResources(sourceClient);
        //Map<String, List<Resource>> filteredSourceResources = getFilteredResources(sourceResources);

        //FilterIterable filterIterable = new FilterIterable(this.options.getBlackListFilter());


        // TODO: BUG; code not proceeding to below
        try {
            Date startDateTime = new Date();
            logger.info(" started processing at {}", startDateTime);

            readAndWriteResourceDocuments(sourceClient, targetClient);


            Date endDateTime = new Date();
            logger.info(" completed processing at {}", endDateTime);
            logger.info(" total time to process is {}", TimeUnit.SECONDS.convert(endDateTime.getTime() - startDateTime.getTime(), TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            String message = "error in while processing server migration.";
            logger.error(message, e);
            throw new AppException(message, e);
        }
        sourceClient.close();
        logger.info("Absolutely nothing should be here after this line");
    }

    private void readAndWriteResourceDocuments(MongoClient sourceClient, MongoClient targetClient) {
        // Note: Working; get the list of databases
        new DatabaseFlowable(sourceClient)
                .filter(db -> {
                    String database = "social";
                    logger.info("db.name: [{}], string: [{}], comparision: [{}]", db.getString("name"), database, db.getString("name").equals(database));
                    return db.getString("name").equalsIgnoreCase(database);
                })
                // Note: Working; for each database get the list of collections in it
                .flatMap(db -> {
                    logger.info(" => found database {}", db.getString("name"));
                    return new CollectionFlowable(sourceClient, db.getString("name"));
                    // Note: Working; CollectionFlowable::subscribeActual works as well
                })
                .filter(resource -> {
                    String collection = "people";
                    logger.info("collection.name: [{}], string: [{}], comparision: [{}]",
                            resource.getCollection(), collection, resource.getCollection().equals(collection));
                    return resource.getCollection().equalsIgnoreCase(collection);
                })
                .map(resource -> {
                    // Note: Nothing in here gets executed
                    logger.info(" ====> map -> found resource {}", resource.toString());
                    return new DocumentReader(sourceClient, resource);
                    //return resource;
                })
                .map(reader -> {
                    logger.info(" ====> reader -> found resource {}", reader.getResource());
                    return new DocumentWriter(targetClient, reader);
                })
                .subscribe(writer -> {
                    // Note: Nothing in here gets executed
                    logger.info(" ====> writer -> found resource {}", writer);
                    writer.blockingLast();
                });
    }

    private boolean isEntireDatabaseBlackListed(String database) {
        logger.info(this.options.getBlackListFilter().toString());
        return this.options.getBlackListFilter()
                    .stream()
                    .anyMatch(bl ->
                            bl.getDatabase().equals(database) &&
                            bl.isEntireDatabase());
    }

    private MongoClient getMongoClient(String cluster) {
        String connectionString = String.format("mongodb://%s", cluster);
        MongoClientURI uri = new MongoClientURI(connectionString);
        return new MongoClient(uri);
    }

    private MongoClient getSourceMongoClient() {
        return getMongoClient(this.options.getSourceCluster());
    }

    private MongoClient getTargetMongoClient() {
        return getMongoClient(this.options.getTargetCluster());
    }

    private Map<String, List<Resource>> getFilteredResources(Map<String, List<Resource>> resources) {
        List<ResourceFilter> blacklist = options.getBlackListFilter();
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
