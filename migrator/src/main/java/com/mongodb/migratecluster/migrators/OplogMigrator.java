package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.oplog.OplogGapWatcher;
import com.mongodb.migratecluster.oplog.OplogReader;
import com.mongodb.migratecluster.oplog.OplogWriter;
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

    private String migratorName;

    private OplogGapWatcher watcher;

    public OplogMigrator(ApplicationOptions options) {
        super(options);
        // NOTE: assuming that source is always replicaSet here
        this.migratorName = options.getSourceCluster();
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
        BsonTimestamp timestamp = getTargetLatestOplogTimestamp();
        if (timestamp == null) {
            logger.info("no oplog entry is found for the shard: [{}]", migratorName);
        }
        else {
            logger.info("found the latest oplog entry for shard: [{}] with timestamp: [{}]", migratorName, timestamp);
        }
        ((Runnable) () -> createGapWatcher()).run();
        this.readSourceAndWriteTarget(timestamp);
    }

    /**
     * A method that is invoked before the actual migration process
     */
    @Override
    public void preprocess() {
        // TODO: ensure oplog timestamp entries are stored in oplog store
    }


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

    private BsonTimestamp getTargetLatestOplogTimestamp() {
        MongoClient client = getOplogClient();
        BsonTimestamp timestamp = getOplogTrackerTimestamp(client, this.migratorName);
        client.close();
        return timestamp;
    }

    /**
     * Get's the saved oplog timestamp for the given migrator
     *
     * @param client a MongoDB client object to work with collections
     * @param reader a string representation of the migrator name
     * @return
     */
    private BsonTimestamp getOplogTrackerTimestamp(MongoClient client, String reader) {
        // NOTE: local.oplog.rs contains the oplog entries as it replicates
        // However when we read from source, we have to perform the operation
        // on target's primary as a new operation. Because of this the oplog
        // time in target would not be same as the one in source. So, track
        // the optime separately in another collection per each shard
        MongoCollection<Document> collection =
                MongoDBHelper.getCollection(client, "migrate-mongo", "oplog.tracker");

        Document query = new Document("reader", reader);
        MongoCursor<Document> cursor =
            collection
                .find(query)
                .sort(new Document("$natural", -1))
                .limit(1)
                .iterator();

        BsonTimestamp ts = null;
        if (cursor.hasNext()){
            ts = cursor.next().get("ts", BsonTimestamp.class);
        }
        return ts;
    }
}
