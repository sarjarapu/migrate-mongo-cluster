package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.oplog.OplogGapWatcher;
import com.mongodb.migratecluster.oplog.OplogReader;
import com.mongodb.migratecluster.oplog.OplogWriter;
import com.mongodb.migratecluster.trackers.*;
import io.reactivex.schedulers.Schedulers;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: OplogMigrator
 * Author: shyam.arjarapu
 * Date: 6/5/17 11:34 AM
 * Description:
 */
public class OplogMigrator extends BaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(OplogMigrator.class);

    private OplogGapWatcher watcher;

    private final Resource oplogTrackerResource;
    private final Resource oplogRsResource;

    public OplogMigrator(ApplicationOptions options) {
        super(options);
        oplogTrackerResource = new Resource("migrate-mongo", "oplog.tracker");
        oplogRsResource = new Resource("local", "oplog.rs");
    }

    /**
     *
     * A process method that implements actual migration
     * of oplog data from source to target.
     *
     * @throws AppException
     * @see AppException
     */
    @Override
    public void process() throws AppException {
//        BsonTimestamp timestamp = getMigatorsLatestOplogTimestamp();
//        if (timestamp == null) {
//            logger.info("no oplog entry is found for the shard: [{}]", migratorName);
//        }
//        else {
//            logger.info("found the latest oplog entry for shard: [{}] with timestamp: [{}]", migratorName, timestamp);
//        }
//        ((Runnable) () -> createGapWatcher()).run();
//        this.readSourceAndWriteTarget(timestamp);
    }

    /**
     * A method that is invoked before the actual migration process
     */
    @Override
    public void preprocess() {
        dropTargetCollectionIfRequired(this.getOplogClient());
        saveSourceOplogTimeIfNotExists();
    }

    private void saveSourceOplogTimeIfNotExists() {
        // TODO
        // if drop, recreate using latest oplog from source
        // else check if an entry already exists.
        // if exists , do nothing
        // else make an entry

        if (options.isDropTarget()) {
            fetchRecentEntryFromSourceAndSaveToOplogstore();
        }
        else {
            // get latest timestamp from oplog store
            BsonTimestamp timestamp = getTimestampFromOplogStore();
            if (timestamp == null) {
                fetchRecentEntryFromSourceAndSaveToOplogstore();
            }
        }
    }

    /**
     * Fetches the recent oplog entry from source and saves onto the oplog store
     */
    private void fetchRecentEntryFromSourceAndSaveToOplogstore() {
        Document document = getLatestOplogEntryFromSource();
        saveTimestampToOplogStore(document);
    }

    /**
     * Get's the saved oplog timestamp on oplog store
     *
     * @return a oplog timestamp fetched from the oplog store
     */
    private BsonTimestamp getTimestampFromOplogStore() {
        if (options.isDropTarget()) {
            return null;
        }
        else {
            MongoClient client = this.getOplogClient();
            ReadOnlyTracker tracker = new OplogTimestampTracker(client, oplogTrackerResource, this.migratorName);
            Document document = tracker.getLatestDocument();
            client.close();
            if (document == null) {
                return null;
            }
            return document.get("ts", BsonTimestamp.class);
        }
    }

    /**
     * Get's the most recent oplog entry from source
     *
     * @return a document representing oplog entry fetched from the source
     */
    private Document getLatestOplogEntryFromSource() {
        MongoClient client = this.getSourceClient();
        ReadOnlyTracker tracker = new OplogTimestampReader(client, oplogRsResource, this.migratorName);
        Document document = tracker.getLatestDocument();
        client.close();
        return document;
    }

    /**
     * Save's a document as the lastest oplog timestamp on oplog store
     *
     * @param document a document representing the fields that need to be set
     */
    private void saveTimestampToOplogStore(Document document) {
        MongoClient client = this.getOplogClient();
        WritableDataTracker tracker = new OplogTimestampTracker(client, oplogTrackerResource, this.migratorName);
        tracker.updateLatestDocument(document);
        client.close();
    }

    /**
     * Drop the collection on target server if configured to drop existing collections.
     *
     * @param client a MongoDB client object to work with collections
     */
    private void dropTargetCollectionIfRequired(MongoClient client) {
        if (options.isDropTarget()) {
            MongoDBHelper.dropCollection(client,
                    oplogTrackerResource.getDatabase(),
                    oplogTrackerResource.getCollection());
        }
    }


//    /**
//     * Get's the saved oplog timestamp for the given migrator
//     *
//     * @param client a MongoDB client object to work with collections
//     * @param reader a string representation of the migrator name
//     * @return
//     */
//    private BsonTimestamp getOplogTrackerTimestamp(MongoClient client, String reader) {
//        // NOTE: local.oplog.rs contains the oplog entries as it replicates
//        // However when we read from source, we have to perform the operation
//        // on target's primary as a new operation. Because of this the oplog
//        // time in target would not be same as the one in source. So, track
//        // the optime separately in another collection per each shard
//        MongoCollection<Document> collection =
//                MongoDBHelper.getCollection(client, "migrate-mongo", "oplog.tracker");
//
//        Document query = new Document("reader", reader);
//        MongoCursor<Document> cursor =
//                collection
//                        .find(query)
//                        .sort(new Document("$natural", -1))
//                        .limit(1)
//                        .iterator();
//
//        BsonTimestamp ts = null;
//        if (cursor.hasNext()){
//            ts = cursor.next().get("ts", BsonTimestamp.class);
//        }
//        return ts;
//    }


    private void readSourceAndWriteTarget(BsonTimestamp lastTimestamp) {
        MongoClient sourceClient = getSourceClient();
        MongoClient targetClient = getTargetClient();
        MongoClient oplogStoreClient = getOplogClient();
        OplogReader reader = new OplogReader(sourceClient, lastTimestamp);
        OplogWriter writer = new OplogWriter(targetClient, oplogStoreClient, this.migratorName);

        reader.subscribe(op -> {
            writer.applyOperation(op);
        });
    }

    private void createGapWatcher() {
        MongoClient sourceClient = getSourceClient();
        MongoClient oplogStoreClient = getOplogClient();

        watcher = new OplogGapWatcher(sourceClient, oplogStoreClient, this.migratorName);
        watcher
                .subscribeOn(Schedulers.newThread())
                .subscribe(gap -> {
                    logger.info(gap.toString());
                });
    }

}
