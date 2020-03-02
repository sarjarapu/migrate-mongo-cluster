package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.observables.OplogBufferedReader;
import com.mongodb.migratecluster.oplog.OplogGapWatcher;
import com.mongodb.migratecluster.oplog.OplogWriter;
import com.mongodb.migratecluster.trackers.*;
import io.reactivex.schedulers.Schedulers;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * File: OplogMigrator
 * Author: Shyam Arjarapu
 * Date: 1/14/17 6:20 AM
 * Description:
 *
 * A class to migrate all the oplog entries from source to oplog store
 *
 */
public class OplogMigrator extends BaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(OplogMigrator.class);

    private OplogGapWatcher watcher;

    private final Resource oplogTrackerResource;
    private final Resource oplogRsResource;
    private Runnable gapWatcher;
    private Long batchCount;
    private Long saveFrequency;

    public OplogMigrator(ApplicationOptions options) {
        super(options);
        oplogTrackerResource = new Resource("migrate-mongo", "oplog.tracker");
        oplogRsResource = new Resource("local", "oplog.rs");
        batchCount = 0L;
        saveFrequency = options.getSaveFrequency();
        
    }

    /**
     * A method that is invoked before the actual migration process
     */
    @Override
    public void preprocess() {
        dropTargetCollectionIfRequired(this.getOplogClient());
        saveSourceOplogTimeOnOplogstoreIfNotExists();
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
        // assumes that timestamp is stored in oplog store from pre-process stage
        BsonTimestamp timestamp = getTimestampFromOplogStore();
        gapWatcher = () -> createGapWatcher();
        gapWatcher.run();
        this.copyOplogsFromSourceToOplogstore(timestamp);
    }

    /**
     * Save's the most recent oplog timestamp on oplog store for current reader if not saved already
     */
    private void saveSourceOplogTimeOnOplogstoreIfNotExists() {
        if (options.isDropTarget()) {
            fetchRecentEntryFromSourceAndSaveToOplogstore();
        }
        else {
            // always get latest timestamp from oplog store.
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
        MongoClient client = this.getOplogClient();
        ReadOnlyTracker tracker = new OplogTimestampTracker(client, oplogTrackerResource, this.migratorName);
        Document document = tracker.getLatestDocument();
        client.close();
        if (document == null) {
            return null;
        }
        return document.get("ts", BsonTimestamp.class);
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
    	logger.debug("Update Oplog Store called call {}.",this.batchCount);
    	if (this.batchCount % this.saveFrequency == 0L) {
	        MongoClient client = this.getOplogClient();
	        WritableDataTracker tracker = new OplogTimestampTracker(client, oplogTrackerResource, this.migratorName);
	        tracker.updateLatestDocument(document);
	        client.close();
	        logger.debug("Oplog Tracker updated");
    	}
    	this.batchCount++;
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

    /**
     * Copies all the oplog entries since the given timestamp from source to oplog store
     *
     * @param lastTimestamp a timestamp on source
     */
    private void copyOplogsFromSourceToOplogstore(BsonTimestamp lastTimestamp) {
        MongoClient sourceClient = getSourceClient();
        MongoClient targetClient = getTargetClient();
        MongoClient oplogStoreClient = getOplogClient();
        logger.info("copyOplogsFromSourceToOplogstore timestamp: {}", lastTimestamp);
        OplogBufferedReader reader = new OplogBufferedReader(sourceClient, lastTimestamp);
        OplogWriter writer = new OplogWriter(targetClient, oplogStoreClient, this.migratorName, this.options);

        reader
                .subscribe(ops -> writer.applyOperations(ops));
    }

    /**
     * Creates a watcher that notifies the gap between oplog entries on source to oplog store
     */
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
