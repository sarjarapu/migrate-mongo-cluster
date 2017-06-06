package com.mongodb.migratecluster.observables;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.migrators.DataMigrator;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.omg.CORBA.portable.ApplicationException;
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
    private final MongoClient client;
    private final String name;

    final static Logger logger = LoggerFactory.getLogger(DataMigrator.class);

    public OplogWriter(MongoClient client, MongoClient oplogStoreClient, String name) {
        this.client = client;
        this.oplogStoreClient = oplogStoreClient;
        this.name = name;
    }

    public void applyOperation(Document operation) throws AppException {
        identifyAndPerformOperation(operation);

        // update the lastOplogTimestamp entry in  'migrate-mongo.oplog.tracker' collection
        updateLastOplogTimestamp(operation);
    }

    private void identifyAndPerformOperation(Document operation) throws AppException {
        try{
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
                case "n":
                    // no op; do nothing, just eat it
                    break;
                case "c":
                    // create table indexes etc operations
                    break;
                default:
                    throw new AppException(String.format(" unsupported operation %s", operation.getString("op")));
            }
        }
        catch (MongoWriteException we) {
            if (we.getMessage().startsWith("E11000 duplicate key error collection")) {
                logger.warn(" [IGNORE] Duplicate key exception while performing operation: {}; error: {}",
                        operation.toJson(), we.toString());
                return;
            }
            logger.error(" error while performing operation: {}; error: {}", operation.toJson(), we.toString());
            throw we;
        }
        catch (Exception e) {
            logger.error(" error while performing operation: {}; error: {}", operation.toJson(), e.toString());
            throw e;
        }
    }

    private void performInsert(Document operation) {
        Document document = operation.get("o", Document.class);
        String ns = operation.getString("ns");

        MongoCollection<Document> collection = this.getCollectionByNamespace(ns);
        collection.insertOne(document);

        String message = String.format(" completed insert op on namespace: %s; document: %s", ns, operation.toJson());
        logger.debug(message);
    }

    private void performUpdate(Document operation) {
        Document find = operation.get("o2", Document.class);
        Document update = operation.get("o", Document.class);
        String ns = operation.getString("ns");

        // what about the options?
        MongoCollection<Document> collection = this.getCollectionByNamespace(ns);
        collection.updateOne(find, update);

        String message = String.format(" completed update op on namespace: %s; document: %s", ns, operation.toJson());
        logger.debug(message);
    }

    private void performDelete(Document operation) {
        Document find = operation.get("o", Document.class);
        String ns = operation.getString("ns");

        // what about the options?
        MongoCollection<Document> collection = this.getCollectionByNamespace(ns);
        collection.deleteOne(find);

        String message = String.format(" completed delete op on namespace: %s; document: %s", ns, operation.toJson());
        logger.debug(message);
    }

    private MongoCollection<Document> getCollectionByNamespace(String ns) {
        String[] parts = ns.split("\\.");
        String databaseName = parts[0];
        String collectionName = ns.substring(databaseName.length()+1);

        return MongoDBHelper.getCollection(client, databaseName, collectionName);
    }

    private void updateLastOplogTimestamp(Document operation) {
        //update back the entry on the oplog tracker
        MongoCollection<Document> collection = MongoDBHelper.getCollection(oplogStoreClient,
                "migrate-mongo", "oplog.tracker");
        Document find = new Document("reader", this.name);
        BsonTimestamp timestamp = operation.get("ts", BsonTimestamp.class);
        Document update = new Document("$set", new Document("ts", timestamp));

        UpdateOptions options = new UpdateOptions().upsert(true);
        collection.updateOne(find, update, options);

        String message = String.format(" updating the oplogStore> migrate-mongo.oplog.tracker with op.ts: %s", timestamp);
        logger.debug(message);
    }
}
