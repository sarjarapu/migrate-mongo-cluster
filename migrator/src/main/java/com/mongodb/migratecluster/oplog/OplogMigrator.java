package com.mongodb.migratecluster.oplog;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.migrators.BaseMigrator;
import io.reactivex.schedulers.Schedulers;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * File: OplogMigrator
 * Author: shyam.arjarapu
 * Date: 6/5/17 11:34 AM
 * Description:
 */
public class OplogMigrator extends BaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(OplogMigrator.class);
    private String sourceMigratorName;

    private OplogGapWatcher oplogGapWatcher;

    public OplogMigrator(ApplicationOptions options) {
        super(options);
        // NOTE: assuming that source is always replicaSet here
        this.sourceMigratorName = options.getSourceCluster();
    }

    @Override
    public void process() throws AppException {
        BsonTimestamp timestamp = getTargetLatestOplogTimestamp();
        if (timestamp == null) {
            logger.info("no oplog entry is found for the shard: [{}]", sourceMigratorName);
        }
        else {
            logger.info("found the latest oplog entry for shard: [{}] with timestamp: [{}]", sourceMigratorName, timestamp);
        }
        ((Runnable) () -> createGapWatcher()).run();
        this.readSourceAndWriteTarget(timestamp);
    }


    private void readSourceAndWriteTarget(BsonTimestamp lastTimestamp) {
        MongoClient sourceClient = getSourceMongoClient();
        MongoClient targetClient = getTargetMongoClient();
        MongoClient oplogStoreClient = getOplogStoreMongoClient();
        OplogReader reader = new OplogReader(sourceClient, lastTimestamp);
        OplogWriter writer = new OplogWriter(targetClient, oplogStoreClient, this.sourceMigratorName);

        reader.subscribe(op -> {
            writer.applyOperation(op);
        });
    }

    private void createGapWatcher() {
        MongoClient sourceClient = getSourceMongoClient();
        MongoClient oplogStoreClient = getOplogStoreMongoClient();

        oplogGapWatcher = new OplogGapWatcher(sourceClient, oplogStoreClient, this.sourceMigratorName);
        oplogGapWatcher
                .subscribeOn(Schedulers.newThread())
                .subscribe(gap -> {
                    logger.info(gap.toString());
                });
    }

    private BsonTimestamp getTargetLatestOplogTimestamp() {
        MongoClient oplogStoreClient = getOplogStoreMongoClient();
        BsonTimestamp timestamp = getLatestOplogTimestamp(oplogStoreClient);
        oplogStoreClient.close();
        return timestamp;
    }

    private BsonTimestamp getLatestOplogTimestamp(MongoClient client) {
        // NOTE: local.oplog.rs contains the oplog entries as it replicates
        // However when we read from source, we have to perform the operation
        // on target's primary as a new operation. Because of this the oplog
        // time in target would not be same as the one in source. So, track
        // the optime separately in another collection per each shard
        MongoCollection<Document> collection =
                MongoDBHelper.getCollection(client, "migrate-mongo", "oplog.tracker");

        Document query = new Document("reader", this.sourceMigratorName);
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
