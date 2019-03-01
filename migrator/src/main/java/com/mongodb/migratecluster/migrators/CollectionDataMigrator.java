package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.DocumentsBatch;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.observables.CollectionFlowable;
import com.mongodb.migratecluster.observables.DatabaseFlowable;
import com.mongodb.migratecluster.observables.DocumentReader;
import com.mongodb.migratecluster.observables.DocumentWriter;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;
import com.mongodb.migratecluster.trackers.CollectionDataTracker;
import com.mongodb.migratecluster.trackers.ReadOnlyTracker;
import com.mongodb.migratecluster.trackers.WritableDataTracker;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * File: CollectionDataMigrator
 * Author: Shyam Arjarapu
 * Date: 1/12/19 9:50 AM
 * Description:
 *
 * A class to help migrate collection data from source to target
 */
public class CollectionDataMigrator extends BaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(CollectionDataMigrator.class);

    public CollectionDataMigrator(ApplicationOptions options) {
        super(options);
    }

    /**
     *
     * A process method that implements actual migration
     * of collection data from source to target.
     *
     * @throws AppException
     * @see AppException
     */
    @Override
    public void process() throws AppException {
        // check if the options are valid
        if (!this.isValidOptions()) {
            String message = String.format("invalid input args for sourceCluster, targetCluster and oplog. \ngiven: %s", this.options.toString());
            throw new AppException(message);
        }
        // loop through source and copy to target
        readSourceClusterDatabases();
    }

    /**
     * A method that is invoked before the actual migration process
     */
    @Override
    public void preprocess() {
        // do nothing
    }

    private void readSourceClusterDatabases() throws AppException {
        MongoClient sourceClient = getSourceClient();
        MongoClient targetClient = getTargetClient();
        MongoClient oplogClient = getOplogClient();

        try {
            Date startDateTime = new Date();
            logger.info("started processing at {}", startDateTime);

            readAndWriteDocuments(sourceClient, targetClient, oplogClient);

            Date endDateTime = new Date();
            logger.info("completed processing at {}", endDateTime);
            logger.info("total time to process is {}", TimeUnit.SECONDS.convert(endDateTime.getTime() - startDateTime.getTime(), TimeUnit.MILLISECONDS));

        } catch (Exception e) {
            String message = "error in while processing server migration.";
            logger.error(message, e);
            throw new AppException(message, e);
        }
        logger.info("Absolutely nothing should be here after this line");
    }

    private void readAndWriteDocuments(MongoClient sourceClient,
                                       MongoClient targetClient,
                                       MongoClient oplogClient) {
        // load the blacklist filters and create database and collection predicates
        List<ResourceFilter> blacklistFilter = options.getBlackListFilter();
        DatabaseFilterPredicate databasePredicate = new DatabaseFilterPredicate(blacklistFilter);
        CollectionFilterPredicate collectionPredicate = new CollectionFilterPredicate(blacklistFilter);

        new DatabaseFlowable(sourceClient)
                .filter(databasePredicate)
                .flatMap(db -> {
                    logger.info("found database: {}", db.getString("name"));
                    return new CollectionFlowable(sourceClient, db.getString("name"));
                })
                .filter(collectionPredicate)
                .map(resource -> {
                    logger.info("found collection {}", resource.getNamespace());
                    dropTargetCollectionIfRequired(targetClient, resource);
                    Document latestDocumentId = getLatestDocumentId(oplogClient, resource);
                    return new DocumentReader(sourceClient, resource, latestDocumentId);
                })
                .map(reader -> new DocumentWriter(targetClient, reader))
                .subscribe(writer -> {
                    writer
                        .map((DocumentsBatch batch) -> {
                            saveLastDocumentInBatch(oplogClient, batch);
                            return batch;
                        })
                        .blockingLast();
                });
        
        sourceClient.close();
        targetClient.close();
    }

    /**
     * Saves the last document from the batch to the oplog database for tracking
     *
     * @param batch an object representing current batch of data
     * @see DocumentsBatch
     *
     */
    private void saveLastDocumentInBatch(MongoClient client, DocumentsBatch batch) {
        if (batch.getSize() == 0) {
            return ;
        }
        Document document = batch.getDocuments().get(batch.getSize()-1);
        logger.info("Saving Batch {}. lastDocumentId [{}]", batch.toString(), document.get("_id"));

        WritableDataTracker tracker = new CollectionDataTracker(client, batch.getResource(), this.migratorName);
        tracker.updateLatestDocument(document);
    }

    /**
     * @param client a MongoDB client object to work with collections
     * @param resource a collection in a database
     * @return a Document representation of the latest document saved into oplog db
     * @see Document
     */
    private Document getLatestDocumentId(MongoClient client, Resource resource) {
        if (options.isDropTarget()) {
            return null;
        }
        else {
            ReadOnlyTracker tracker = new CollectionDataTracker(client, resource, this.migratorName);
            return tracker.getLatestDocument();
        }
    }

    /**
     * Drop the collection on target server if configured to drop existing collections.
     *
     * @param client a MongoDB client object to work with collections
     * @param resource a collection in a database
     */
    private void dropTargetCollectionIfRequired(MongoClient client, Resource resource) {
        if (options.isDropTarget()) {
            MongoDBHelper.dropCollection(client, resource.getDatabase(), resource.getCollection());
        }
    }

}
