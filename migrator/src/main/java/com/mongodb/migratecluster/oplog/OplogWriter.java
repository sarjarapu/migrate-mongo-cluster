package com.mongodb.migratecluster.oplog;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.trackers.OplogTimestampTracker;
import com.mongodb.migratecluster.trackers.WritableDataTracker;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * File: OplogWriter
 * Author: Shyam Arjarapu
 * Date: 1/14/19 7:20 AM
 * Description:
 *
 * A class to help write the apply the oplog entries on the target
 */
public class OplogWriter {
    private final MongoClient oplogStoreClient;
    private final MongoClient targetClient;
    private final String reader;
    private final Resource oplogTrackerResource;

    final static Logger logger = LoggerFactory.getLogger(OplogWriter.class);

    public OplogWriter(MongoClient targetClient, MongoClient oplogStoreClient, String reader) {
        this.targetClient = targetClient;
        this.oplogStoreClient = oplogStoreClient;
        this.reader = reader;
        oplogTrackerResource = new Resource("migrate-mongo", "oplog.tracker");
    }

    /**
     * Applies the oplog documents on the oplog store
     *
     * @param operations a list of oplog operation documents
     * @throws AppException
     */
    public int applyOperations(List<Document> operations) throws AppException {
        int totalModelsAdded = 0;
        String previousNamespace = null;
        Document previousDocument = null;
        List<WriteModel<Document>> models = new ArrayList<>();

        for(int i = 0; i < operations.size(); i++) {
            Document currentDocument = operations.get(i);
            String currentNamespace = currentDocument.getString("ns");

            if (!currentNamespace.equals(previousNamespace)) {
                // change of namespace. bulk apply models for previous namespace
                if (previousNamespace != null && models.size() > 0) {
                    BulkWriteResult bulkWriteResult = applyBulkWriteModelsOnCollection(previousNamespace, models);
                    totalModelsAdded += bulkWriteResult.getDeletedCount() + bulkWriteResult.getModifiedCount() + bulkWriteResult.getInsertedCount();
                    models.clear();
                    // save documents timestamp to oplog tracker
                    saveTimestampToOplogStore(previousDocument);
                }
                previousNamespace = currentNamespace;
                previousDocument = currentDocument;
            }
            WriteModel<Document> model = getWriteModelForOperation(currentDocument);
            if (model != null) {
                models.add(model);
            }
            else {
                logger.error("=====> could not convert the document to model.");
            }
        }

        if (models.size() > 0) {
            BulkWriteResult bulkWriteResult = applyBulkWriteModelsOnCollection(previousNamespace, models);
            totalModelsAdded += bulkWriteResult.getDeletedCount() + bulkWriteResult.getModifiedCount() + bulkWriteResult.getInsertedCount();

            // save documents timestamp to oplog tracker
            saveTimestampToOplogStore(previousDocument);
        }

        if (totalModelsAdded != operations.size()) {
            logger.error("total models added {} is not equal to operations injected {}", totalModelsAdded, operations.size());
        }

        return totalModelsAdded;
    }

    private BulkWriteResult applyBulkWriteModelsOnCollection(String namespace,
                                 List<WriteModel<Document>> operations)  throws AppException {
        MongoCollection<Document> collection = MongoDBHelper.getCollectionByNamespace(this.targetClient, namespace);
        BulkWriteResult writeResult = MongoDBHelper.performOperationWithRetry(
                () -> {
                    BulkWriteOptions options = new BulkWriteOptions();
                    options.ordered(true);
                    return collection.bulkWrite(operations, options);
                }
                , new Document("operation", "bulkWrite"));
        return writeResult;
    }

    /**
     * Get's a WriteModel for the given oplog operation
     *
     * @param operation an oplog operation
     * @return a WriteModel of a bulk operation
     */
    private WriteModel<Document> getWriteModelForOperation(Document operation)  throws AppException {
        String message;
        WriteModel<Document> model = null;
        switch (operation.getString("op")){
            case "i":
                model = getInsertWriteModel(operation);
                break;
            case "u":
                model = getUpdateWriteModel(operation);
                break;
            case "d":
                model = getDeleteWriteModel(operation);
                break;
            case "c":
                // might have to be individual operation
                performRunCommand(operation);
                //TODO performRunCommand
                // update the last timestamp on oplogStore
                break;
            case "n":
                break;
            default:
                message = String.format("unsupported operation %s; op: %s", operation.getString("op"), operation.toJson());
                logger.error(message);
                throw new AppException(message);
        }
        return model;
    }

    private WriteModel<Document> getInsertWriteModel(Document operation) {
        Document document = operation.get("o", Document.class);
        return new InsertOneModel<>(document);
    }

    private WriteModel<Document>  getUpdateWriteModel(Document operation) throws AppException {
        Document find = operation.get("o2", Document.class);
        Document update = operation.get("o", Document.class);

        return new UpdateOneModel<>(find, update);
    }

    private WriteModel<Document>  getDeleteWriteModel(Document operation) throws AppException {
        Document find = operation.get("o", Document.class);
        return new DeleteOneModel<>(find);
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

    /**
     * Save's a document as the lastest oplog timestamp on oplog store
     *
     * @param document a document representing the fields that need to be set
     */
    protected void saveTimestampToOplogStore(Document document) {
        WritableDataTracker tracker = new OplogTimestampTracker(oplogStoreClient, oplogTrackerResource, this.reader);
        tracker.updateLatestDocument(document);
    }
}
