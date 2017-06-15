package com.mongodb.migratecluster.oplog;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.migrators.DataMigrator;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: OplogWriter
 * Author: shyam.arjarapu
 * Date: 6/5/17 11:34 AM
 * Description:
 */
public class OplogWriter {
    private final MongoClient oplogStoreClient;
    private final MongoClient targetClient;
    private final String name;

    final static Logger logger = LoggerFactory.getLogger(OplogWriter.class);

    public OplogWriter(MongoClient targetClient, MongoClient oplogStoreClient, String name) {
        this.targetClient = targetClient;
        this.oplogStoreClient = oplogStoreClient;
        this.name = name;
    }

    public void applyOperation(Document operation) throws AppException {
        // wait once for write operations if no primary
        identifyAndPerformOperation(operation);

        // update the lastOplogTimestamp entry in  'migrate-mongo.oplog.tracker' collection
        updateLastOplogTimestamp(operation);
    }

    private void identifyAndPerformOperation(Document operation) throws AppException {
        String message;
        switch (operation.getString("op")){
                case "i":
                    performInsert(operation);
                    break;
                case "u":
                    performUpdate(operation);
                    break;
                case "d":
                    performDelete(operation);
                    break;
                case "c":
                    performRunCommand(operation);
                    break;
                case "n":
                    // no op; do nothing, just eat it
                    break;
                default:
                    message = String.format("unsupported operation %s; op: %s", operation.getString("op"), operation.toJson());
                    logger.error(message);
                    throw new AppException(message);
            }
    }

    private void performInsert(Document operation) throws AppException {
        Document document = operation.get("o", Document.class);
        String ns = operation.getString("ns");

        MongoCollection<Document> collection = MongoDBHelper.getCollectionByNamespace(this.targetClient, ns);
        MongoDBHelper.performOperationWithRetry(() -> {
            collection.insertOne(document);
            return 1L;
        }, operation);

        String message = String.format("completed insert op on namespace: %s; document: %s", ns, operation.toJson());
        logger.debug(message);
    }

    private void performUpdate(Document operation) throws AppException {
        Document find = operation.get("o2", Document.class);
        Document update = operation.get("o", Document.class);
        String ns = operation.getString("ns");

        // what about the options?
        MongoCollection<Document> collection = MongoDBHelper.getCollectionByNamespace(this.targetClient, ns);
        MongoDBHelper.performOperationWithRetry(() -> {
            UpdateResult result = collection.updateOne(find, update);
            return result.getModifiedCount();
        }, operation);

        String message = String.format("completed update op on namespace: %s; document: %s", ns, operation.toJson());
        logger.debug(message);
    }

    private void performDelete(Document operation) throws AppException {
        Document find = operation.get("o", Document.class);
        String ns = operation.getString("ns");

        // what about the options?
        MongoCollection<Document> collection = MongoDBHelper.getCollectionByNamespace(this.targetClient, ns);
        MongoDBHelper.performOperationWithRetry(() -> {
            DeleteResult result = collection.deleteOne(find);
            return result.getDeletedCount();
        }, operation);

        String message = String.format("completed delete op on namespace: %s; document: %s", ns, operation.toJson());
        logger.debug(message);
    }


    private void performRunCommand(Document operation) throws AppException {
        Document document = operation.get("o", Document.class);
        String databaseName = operation.getString("ns").replace(".$cmd", "");

        MongoDatabase database = MongoDBHelper.getDatabase(this.targetClient, databaseName);
        MongoDBHelper.performOperationWithRetry(() -> {
            database.runCommand(document);
            return 1L;
        }, operation);

        String message = String.format("completed runCommand op on database: %s; document: %s", databaseName, operation.toJson());
        logger.debug(message);
    }

    private void updateLastOplogTimestamp(Document operation) throws AppException {
        //update back the entry on the oplog tracker
        Document find = new Document("reader", this.name);
        BsonTimestamp timestamp = operation.get("ts", BsonTimestamp.class);
        Document update = new Document("$set", new Document("ts", timestamp));
        UpdateOptions options = new UpdateOptions().upsert(true);

        MongoCollection<Document> collection = MongoDBHelper.getCollection(oplogStoreClient,
                "migrate-mongo", "oplog.tracker");
        MongoDBHelper.performOperationWithRetry(() -> {
            UpdateResult result = collection.updateOne(find, update, options);
            return result.getModifiedCount();
        }, operation);

        String message = String.format("updating the oplogStore> migrate-mongo.oplog.tracker with op.ts: %s", timestamp);
        logger.debug(message);
    }
}
