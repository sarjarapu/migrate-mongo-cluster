package com.mongodb.migratecluster.observables;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.migratecluster.migrators.MigratorSettings;
import com.mongodb.migratecluster.model.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * File: DocumentIdReader
 * Author: Shyam Arjarapu
 * Date: 1/11/19 5:20 PM
 * Description:
 * This class helps you fetch document _ids of given collection in natural order.
 * if continueFromDocumentId is given then the reader still reads from the first
 * but skips all the _id notification until it comes across _id in continueFromDocumentId
 * the fetched _ids are published in bulk for a reader to read the whole document
 */
public class DocumentIdReader extends Observable<Object> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentIdReader.class);
    private final MongoCollection<Document> collection;
    private final Resource resource;
    private final Document readFromDocumentId;

    /**
     * @param collection
     * @param resource An object representing database and collection that reader will process
     * @param readFromDocumentId A Document representing where to continue reading from given collection
     */
    public DocumentIdReader(MongoCollection<Document> collection, Resource resource, Document readFromDocumentId) {
        this.collection = collection;
        this.resource = resource;
        this.readFromDocumentId = readFromDocumentId;
    }

    /**
     * @param observer
     */
    @Override
    protected void subscribeActual(Observer<? super Object> observer) {
        FindIterable<Document> documents = getDocumentIdsFrom(this.readFromDocumentId);

        boolean readFromNowOn = true;

        if (readFromDocumentId != null) {
            readFromNowOn = false;
            logger.info("found a tracker entry for resource {}. skipping all document until latest document {}", resource, readFromDocumentId.get("_id"));
        }
        AtomicInteger counter = new AtomicInteger(0);
        for (Document item : documents) {
            if (!item.isEmpty()) {
                if (readFromNowOn) {
                    String message = String.format("idReader reading document by _id: [%s]", item.get("_id").toString());
                    logger.debug(message);
                    observer.onNext(item.get("_id"));
                }
                else {
                    counter.addAndGet(1);
                    logger.debug("skipping current document {}", item.get("_id"));
                    if (readFromDocumentId.get("latest_id").equals(item.get("_id"))) {
                        readFromNowOn = true;
                        logger.info("successfully found latest document after reads {}. Document {}", counter.get(), readFromDocumentId.get("latest_id"));
                    }
                }
            }
        }
        observer.onComplete();
    }

    private FindIterable<Document> getDocumentIdsFrom(Document idValue) {
        if (idValue != null) {
            // find the first _id of a few batches prior to given idValue
            Document idValueFromPriorBatches = collection
                    .find(Filters.lte("_id", idValue.get("latest_id")))
                    .sort(BsonDocument.parse("{_id: -1}")) // descending order
                    .skip((MigratorSettings.BATCHES_MAX_COUNT + 1) * MigratorSettings.BATCH_SIZE_ID_READER) // excess just in case
                    .limit(1)
                    .projection(BsonDocument.parse("{_id: 1}"))
                    .first();

            if (idValueFromPriorBatches != null) {
                // get all the docs from that priorBatch idValue
                FindIterable<Document> iterable = collection
                        .find(Filters.gte("_id", idValueFromPriorBatches.get("_id")))
                        .projection(BsonDocument.parse("{_id: 1}"))
                        .sort(BsonDocument.parse("{_id: 1}"))
                        .batchSize(MigratorSettings.BATCH_SIZE_ID_READER);
                return iterable;
            }
        }

        FindIterable<Document> iterable = collection
                .find()
                .projection(BsonDocument.parse("{_id: 1}"))
                .sort(BsonDocument.parse("{_id: 1}"))
                .batchSize(MigratorSettings.BATCH_SIZE_ID_READER);
        return iterable;
    }
}
