package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.observables.*;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.Document;
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
        // option values for source, target, oplog must not be blank
        if ((this.options.getSourceCluster().equals("")) ||
            (this.options.getTargetCluster().equals("")) ||
            (this.options.getOplogStore().equals(""))) {
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

        //DatabaseFilterPredicate filterIterable = new DatabaseFilterPredicate(this.options.getBlackListFilter());

        try {
            Date startDateTime = new Date();
            logger.info(" started processing at {}", startDateTime);

            readAndWriteResourceDocuments(sourceClient, targetClient);
            //doATestRun(sourceClient);

            Date endDateTime = new Date();

            logger.info(" completed processing at {}", endDateTime);
            logger.info(" total time to process is {}", TimeUnit.SECONDS.convert(endDateTime.getTime() - startDateTime.getTime(), TimeUnit.MILLISECONDS));

        } catch (Exception e) {
            String message = "error in while processing server migration.";
            logger.error(message, e);
            throw new AppException(message, e);
        }
        logger.info("Absolutely nothing should be here after this line");
    }

    private void doATestRun(MongoClient client) {
        Resource resource = new Resource("social", "user");
        MongoCollection<Document> collection = client
                .getDatabase(resource.getDatabase())
                .getCollection(resource.getCollection());
        DocumentIdReader reader = new DocumentIdReader(collection, resource);
        Observable<List<ResourceDocument>> observable = reader
                .buffer(1000)
                .flatMap(new Function<List<Object>, ObservableSource<List<ResourceDocument>>>() {
                    @Override
                    public ObservableSource<List<ResourceDocument>> apply(List<Object> ids) throws Exception {
                        return new DocumentsObservable(collection, resource, ids.toArray())
                                .subscribeOn(Schedulers.io());
                    }
                });
        observable
            .blockingSubscribe(id -> {
            logger.info("## found docId: {}", id.size());
        });
    }

    private void readAndWriteResourceDocuments(MongoClient sourceClient, MongoClient targetClient) {
        // load the blacklist filters and create database and collection predicates
        List<ResourceFilter> blacklistFilter = options.getBlackListFilter();
        DatabaseFilterPredicate databasePredicate = new DatabaseFilterPredicate(blacklistFilter);
        CollectionFilterPredicate collectionPredicate = new CollectionFilterPredicate(blacklistFilter);

        new DatabaseFlowable(sourceClient)
                .filter(databasePredicate)
                .flatMap(db -> {
                    logger.debug(" found database: {}", db.getString("name"));
                    return new CollectionFlowable(sourceClient, db.getString("name"));
                })
                .filter(collectionPredicate)
                .map(resource -> {
                    logger.info(" ====> map -> found resource {}", resource.toString());
                    return new DocumentReader(sourceClient, resource);
                })
                .map(reader -> {
                    logger.info(" ====> reader -> found resource {}", reader.getResource());
                    return new DocumentWriter(targetClient, reader);
                })
                .subscribe(writer -> {
                    // block on the
                    logger.info(" ====> writer -> found resource {}", writer);
                    writer.blockingLast();
                });
                //TODO: BUG; looks like onNext is happening before the actual Ids are returned
        sourceClient.close();
        targetClient.close();
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
