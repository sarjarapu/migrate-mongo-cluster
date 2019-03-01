package com.mongodb.migratecluster.observables;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
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
    private final int BATCH_SIZE_ID_READER = 10; // 5000

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
        FindIterable<Document> documents = collection
                .find()
                .projection(BsonDocument.parse("{_id: 1}"))
                .sort(BsonDocument.parse("{$natural: 1}"))
                .batchSize(BATCH_SIZE_ID_READER);

        boolean readFromNowOn = true;

        if (readFromDocumentId != null) {
            readFromNowOn = false;
            logger.info("found a tracker entry for resource {}. skipping all document until latest document {}", resource, readFromDocumentId.get("_id"));
        }
        AtomicInteger counter = new AtomicInteger(0);
        for (Document item : documents) {
            if (!item.isEmpty()) {
                // TODO: Turn on the throttling here
                if (readFromNowOn) {
                    String message = String.format("idReader reading document by _id: [%s]", item.get("_id").toString());
                    logger.info(message);
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
}
