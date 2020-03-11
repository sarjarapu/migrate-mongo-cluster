package com.mongodb.migratecluster.oplog;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.helpers.BulkWriteOutput;
import com.mongodb.migratecluster.helpers.ModificationHelper;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;
import com.mongodb.migratecluster.trackers.OplogTimestampTracker;
import com.mongodb.migratecluster.trackers.WritableDataTracker;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Set;

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
    private final HashMap<String, Boolean> allowedNamespaces;

    final static Logger logger = LoggerFactory.getLogger(OplogWriter.class);
    private final DatabaseFilterPredicate databasePredicate;
    private final CollectionFilterPredicate collectionPredicate;
    private final ModificationHelper modificationHelper;
    
    private Long batchCount;
    private Long saveFrequency;

    public OplogWriter(MongoClient targetClient, MongoClient oplogStoreClient, String reader, ApplicationOptions options) {
        this.targetClient = targetClient;
        this.oplogStoreClient = oplogStoreClient;
        this.reader = reader;
        oplogTrackerResource = new Resource("migrate-mongo", "oplog.tracker");
        allowedNamespaces = new HashMap<>();

        List<ResourceFilter> blacklistFilter = options.getBlackListFilter();
        databasePredicate = new DatabaseFilterPredicate(blacklistFilter);
        collectionPredicate = new CollectionFilterPredicate(blacklistFilter);
        modificationHelper = new ModificationHelper(options);
        
        batchCount = 0L;
        saveFrequency = options.getSaveFrequency();
    }

    /**
     * Applies the oplog documents on the oplog store
     *
     * @param operations a list of oplog operation documents
     * @throws AppException
     */
    public int applyOperations(List<Document> operations) throws AppException {
        int totalModelsAdded = 0;
        int totalValidOperations = 0;
        String previousNamespace = null;
        Document previousDocument = null;
        List<WriteModel<Document>> models = new ArrayList<>();

        for(int i = 0; i < operations.size(); i++) {
            Document currentDocument = operations.get(i);
            String currentNamespace = currentDocument.getString("ns");
            // Check for blacklisted namespace in the original deployment
            if (!isNamespaceAllowed(currentNamespace)) {
                continue;
            }

            // modify namespace via namespacesRename
            currentNamespace = modificationHelper.getMappedNamespace(currentDocument.getString("ns"));
            if (!currentNamespace.equals(previousNamespace)) {
                // change of namespace. bulk apply models for previous namespace
                if (previousNamespace != null && models.size() > 0) {
                    BulkWriteOutput output = applyBulkWriteModelsOnCollection(previousNamespace, models);
                    if (models.size() == output.getSuccessfulWritesCount()) {
                        logger.info("all the {} write operations for the {} batch were applied successfully", models.size(), previousNamespace);
                    }
                    else {
                        logger.error("[FATAL] the {} write operations for the {} batch were applied fully; output {}", operations.size(), currentDocument, output.toString());
                    }
                    totalModelsAdded += output.getSuccessfulWritesCount();
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
                totalValidOperations++;
            }
            else {
                // if the command is $cmd for create index or create collection, there would not be any write model.
                logger.warn(String.format("ignoring oplog entry. could not convert the document to model. Given document is [%s]", currentDocument.toJson()));
            }
        }

        if (models.size() > 0) {
            BulkWriteOutput output = applyBulkWriteModelsOnCollection(previousNamespace, models);
            if (output != null) {
            	int numSuccessfull = output.getSuccessfulWritesCount();
            	logger.info(" {} operations applied for namespace {}.", numSuccessfull, previousNamespace);
                totalModelsAdded += numSuccessfull ; // bulkWriteResult.getUpserts().size();
                // save documents timestamp to oplog tracker
                saveTimestampToOplogStore(previousDocument);
            }
        }

        // TODO: What happens if there is duplicate exception?
        if (totalModelsAdded != totalValidOperations) {
            logger.warn("[FATAL] total models added {} is not equal to operations injected {}. grep the logs for BULK-WRITE-RETRY", totalModelsAdded, operations.size());
        }
        else {

        }

        return totalModelsAdded;
    }

    private boolean isNamespaceAllowed(String namespace) {
        if (!allowedNamespaces.containsKey(namespace))
        {
            boolean allow = checkIfNamespaceIsAllowed(namespace);
            allowedNamespaces.put(namespace, allow);
        }
        // return cached value
        return allowedNamespaces.get(namespace);
    }

    private boolean checkIfNamespaceIsAllowed(String namespace) {
        String databaseName = namespace.split("\\.")[0];
        try {
            Document dbDocument = new Document("name", databaseName);
            boolean isNotBlacklistedDB = databasePredicate.test(dbDocument);
            if (isNotBlacklistedDB) {
                // check for collection as well
                String collectionName = namespace.substring(databaseName.length()+1);
                Resource resource = new Resource(databaseName, collectionName);
                return collectionPredicate.test(resource);
            }
            else {
                return false;
            }
        } catch (Exception e) {
            logger.error("error while testing the namespace is in black list or not", e);
            return false;
        }
    }

    private BulkWriteOutput applyBulkWriteModelsOnCollection(String namespace,
                                 List<WriteModel<Document>> operations)  throws AppException {
        MongoCollection<Document> collection = MongoDBHelper.getCollectionByNamespace(this.targetClient, namespace);
        try{
            BulkWriteResult bulkWriteResult = applyBulkWriteModelsOnCollection(collection, operations);
            BulkWriteOutput output = new BulkWriteOutput(bulkWriteResult);
            return output;
        }
        catch (MongoBulkWriteException err) {
/*            if (err.getWriteErrors().size() == operations.size()) {
 *               // every doc in this batch is error. just move on
 *               logger.debug("[IGNORE] Ignoring all the {} write operations for the {} batch as they all failed with duplicate key exception. (already applied previously)", operations.size(), namespace);
 *               return new BulkWriteOutput(0,0,0, 0, operations.size(), new ArrayList<>());
 *           }
 */
            logger.warn("[WARN] the {} bulk write operations for the {} batch failed with exceptions. applying them one by one. error: {}", operations.size(), namespace, err.getWriteErrors().toString());
            return applySoloBulkWriteModelsOnCollection(operations, collection);
        }
        catch (Exception ex) {
            logger.error("[FATAL] unknown exception occurred while apply bulk write options for oplog. err: {}. Going to retry individually anyways", ex.toString());
            return applySoloBulkWriteModelsOnCollection(operations, collection);
        }
    }

    private BulkWriteOutput applySoloBulkWriteModelsOnCollection(List<WriteModel<Document>> operations, MongoCollection<Document> collection) {
        BulkWriteResult soloResult = null;
        List<WriteModel<Document>> failedOps = new ArrayList<>();
        int deletedCount = 0;
        int modifiedCount = 0;
        int insertedCount = 0;
        int upsertedCount = 0;
        for (WriteModel<Document> op : operations) {
            List<WriteModel<Document>> soloBulkOp = new ArrayList<>();
            soloBulkOp.add(op);
            try {
                soloResult = applyBulkWriteModelsOnCollection(collection, soloBulkOp);
                deletedCount += soloResult.getDeletedCount();
                modifiedCount += soloResult.getModifiedCount();
                insertedCount += soloResult.getInsertedCount();
                upsertedCount += soloResult.getUpserts().size();
              
                logger.info("[BULK-WRITE-RETRY SUCCESS] retried solo op {} on collection: {} produced result: {}",
                        op.toString(), collection.getNamespace().getFullName(), soloResult.toString());
                // no errors? keep going
            } catch (MongoBulkWriteException bwe) {
                BulkWriteError we = bwe.getWriteErrors().get(0);
                if (bwe.getMessage().contains("E11000 duplicate key error collection")) {
                    logger.warn("[BULK-WRITE-RETRY IGNORE] ignoring duplicate key exception for solo op {} on collection: {}; details: {}; error: {}",
                            op.toString(), collection.getNamespace().getFullName(), we.getDetails().toJson(), bwe.toString());
                }
                else {
                    failedOps.add(op);
                    logger.error("[BULK-WRITE-RETRY ERROR] error occurred while applying solo op {} on collection: {}; operation: {}; error {}",
                            op.toString(), collection.getNamespace().getFullName(), we.getDetails().toJson(), bwe.toString());
                }
            } catch (Exception soloErr) {
                failedOps.add(op);
                logger.error("[BULK-WRITE-RETRY] unknown exception occurred while applying solo op {} on collection: {}; error {}",
                        op.toString(), collection.getNamespace().getFullName(), soloErr.toString());
            }
        }
        BulkWriteOutput output = new BulkWriteOutput(deletedCount, modifiedCount, insertedCount, upsertedCount, 0, failedOps); //TODO: upsertedCount ??
        logger.info("[BULK-WRITE-RETRY] all the {} operations for the batch {} were retried one-by-one. result {}",
                operations.size(), collection.getNamespace().getFullName(), output.toString());
        return output;
    }

    private BulkWriteResult applyBulkWriteModelsOnCollection(MongoCollection<Document> collection, List<WriteModel<Document>> operations) throws AppException {
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
                //TODO update the last timestamp on oplogStore?
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
        /*
         * If the oplog is large enough, an oplog insert might have already been applied during
         * initial sync. Setting the operation as insert will throw duplicate key exception during
         * bulk operation, slowing down the overall performance rate. So use replaceOne with upsert.
         */
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        Document find = new Document("_id",document.get("_id"));
        return new ReplaceOneModel<>(find,document,options);
    }

    private WriteModel<Document>  getUpdateWriteModel(Document operation) throws AppException {
        Document find = operation.get("o2", Document.class);
        Document update = operation.get("o", Document.class);
      
        if (update.containsKey("$v")) {
          update.remove("$v");
        }

        // if the update operation is not using $set then use replaceOne
        Set<String> docKeys = update.keySet();
        if (docKeys.size() == 1 && docKeys.iterator().next().startsWith("$"))
          return new UpdateOneModel<>(find, update);
        else
          return new ReplaceOneModel<>(find, update);
    }

    private WriteModel<Document>  getDeleteWriteModel(Document operation) throws AppException {
        Document find = operation.get("o", Document.class);
        return new DeleteOneModel<>(find);
    }

    private void performRunCommand(Document origOperation) throws AppException {
        Document operation = getMappedOperation(origOperation);
        Document document = operation.get("o", Document.class);
        String databaseName = operation.getString("ns").replace(".$cmd", "");

        logger.debug("performRunCommand: {}", databaseName);
        logger.debug("performRunCommand, modified operation: {}", operation);
        MongoDatabase database = MongoDBHelper.getDatabase(this.targetClient, databaseName);
        MongoDBHelper.performOperationWithRetry(() -> {
            database.runCommand(document);
            return 1L;
        }, operation);

        String message = String.format("completed runCommand op on database: %s; document: %s", databaseName, operation.toJson());
        logger.debug(message);
    }

    private Document getMappedOperation(Document operation) {
        Document document = operation.get("o", Document.class);
        if (!document.containsKey("create") &&
                !document.containsKey("drop") &&
                !document.containsKey("create")) {
            return operation;
        }
        updateOperationWithMappedCollectionIfRequired(operation, document,"drop");
        updateOperationWithMappedCollectionIfRequired(operation, document,"create");
        return operation;
    }

    private void updateOperationWithMappedCollectionIfRequired(Document operation, Document document, String operationName ) {
        if (document.containsKey(operationName)) {
            String databaseName = operation.getString("ns").replace(".$cmd", "");
            String collectionName = (String) document.get(operationName);

            Resource mappedResource = modificationHelper.getMappedResource(new Resource(databaseName, collectionName));
            document.put(operationName, mappedResource.getCollection());
            operation.put("ns", mappedResource.getDatabase() + ".$cmd");

            if (operationName == "create") {
                updateIdIndexOperationWithMappedNamespace(document, mappedResource.getNamespace());
            }
        }
    }

    private void updateIdIndexOperationWithMappedNamespace(Document document, String namespace) {
        if (document.containsKey("idIndex")) {
            Document index = document.get("idIndex", Document.class);
            index.put("ns", namespace);
        }
    }

    /**
     * Save's a document as the lastest oplog timestamp on oplog store
     *
     * @param document a document representing the fields that need to be set
     */
    protected void saveTimestampToOplogStore(Document document) {
    	logger.debug("Update Oplog Store2 called call {}.",this.batchCount);
    	if (this.batchCount % this.saveFrequency == 0L) {
	        WritableDataTracker tracker = new OplogTimestampTracker(oplogStoreClient, oplogTrackerResource, this.reader);
	        tracker.updateLatestDocument(document);
	        logger.debug("Oplog2 Tracker updated");
    	}
    	this.batchCount++;
    }
}
