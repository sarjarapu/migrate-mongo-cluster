package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.model.DocumentsBatch;
import com.mongodb.migratecluster.observables.*;
import com.mongodb.migratecluster.oplog.OplogMigrator;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * File: DataMigrator
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:45 PM
 * Description:
 */
public class DataMigrator extends BaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(DataMigrator.class);

    private final OplogMigrator oplogMigrator;

    public DataMigrator(ApplicationOptions options) {
        super(options);

        this.oplogMigrator = new OplogMigrator(options);
    }

    @Override
    public void process() throws AppException {
        // check if the options are valid
        if (!this.isValidOptions()) {
            String message = String.format("invalid input args for sourceCluster, targetCluster and oplog. \ngiven: %s", this.options.toString());
            throw new AppException(message);
        }

        // before you begin the copy make a note of oplog entry
        // if oplog entry already exists do nothing
        // start copying all the data from source to target
        // only after completing the copy, you should begin oplog apply

        // if copy failed in between, use of drop option starts all over again

        // start the oplog tailing
        // this.oplogMigrator.process();

        // loop through source and copy to target
        // NOTE: running the copy without oplog migrator ran correctly.
        // but with oplog migrator it seems to not fully complete
        readSourceClusterDatabases();

        // TODO: when copying is all done, auto replay oplog
    }

    private void readSourceClusterDatabases() throws AppException {
        MongoClient sourceClient = getSourceMongoClient();
        MongoClient targetClient = getTargetMongoClient();

        try {
            Date startDateTime = new Date();
            logger.info("started processing at {}", startDateTime);

            readAndWriteResourceDocuments(sourceClient, targetClient);

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

    private void readAndWriteResourceDocuments(MongoClient sourceClient, MongoClient targetClient) {
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
                    Document latestDocumentId = getLatestDocumentId(targetClient, resource);
                    return new DocumentReader(sourceClient, resource, latestDocumentId);
                })
                .map(reader -> new DocumentWriter(targetClient, reader))
                .subscribe(writer -> {
                    DocumentsBatch resourceDocuments1 = writer
                            .map((DocumentsBatch batch) -> {
                                saveLastDocumentInBatch(batch);
                                return batch;
                            })
                            .blockingLast();
//                    Resource resource = resourceDocuments1.getResource();
//                    logger.info("Make an update to the oplog database about the last entry. Resource {}. Completed inserting docs {}",
//                            resource, resourceDocuments1.getSize());
//                    //return resourceDocuments1;
                });
        
        sourceClient.close();
        targetClient.close();
    }

    private void saveLastDocumentInBatch(DocumentsBatch batch) {
        Document document = batch.getDocuments().get(batch.getSize()-1);
        logger.info("Saving Batch {}. lastDocumentId [{}]", batch.toString(), document.get("_id"));
    }

    private Document getLatestDocumentId(MongoClient client, Resource resource) {
        if (options.isDropTarget()) {
            return null;
        }
        else {
            MongoDatabase database = client.getDatabase(resource.getDatabase());
            MongoCollection<Document> collection = database.getCollection(resource.getCollection());
            FindIterable<Document> documents = collection
                    .find()
                    .projection(BsonDocument.parse("{_id: 1}"))
                    .sort(BsonDocument.parse("{$natural: -1}"))
                    .limit(1);
            return documents.first();
        }
    }

    private void dropTargetCollectionIfRequired(MongoClient targetClient, Resource resource) {
        if (options.isDropTarget()) {

            MongoDatabase database = targetClient.getDatabase(resource.getDatabase());
            MongoCollection<Document> collection = database.getCollection(resource.getCollection());
            collection.drop();

            logger.info("dropping collection {} on target {}",
                    resource.getNamespace(),
                    targetClient.getAddress().toString());
        }
    }

}
