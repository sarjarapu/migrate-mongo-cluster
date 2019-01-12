package com.mongodb.migratecluster.observables;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        FindIterable<Document> documents = collection
                .find()
                .projection(BsonDocument.parse("{_id: 1}"))
                .sort(BsonDocument.parse("{$natural: 1}"))
                .batchSize(5000);

        boolean readFromNowOn = true;

        if (readFromDocumentId != null) {
            readFromNowOn = false;
        }
        for (Document item : documents) {
            if (!item.isEmpty()) {
                // TODO: Turn on the throttling here
                if (readFromNowOn) {
                    String message = String.format("reading document by _id: [%s]", item.get("_id").toString());
                    logger.debug(message);
                    observer.onNext(item.get("_id"));
                }
                else {
                    if (readFromDocumentId.get("_id").equals(item.get("_id"))) {
                        readFromNowOn = true;
                    }
                }
            }
        }
        observer.onComplete();
    }
}
