package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.migratecluster.helpers.ModificationHelper;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.model.DocumentsBatch;
import com.mongodb.migratecluster.observers.BaseDocumentWriter;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Observer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * File: DocumentWriter
 * Author: shyam.arjarapu
 * Date: 4/26/17 5:02 AM
 * Description:
 */
/**
 * @author tspin
 *
 */
public class DocumentWriter extends Observable<DocumentsBatch> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentWriter.class);
    private final DocumentReader documentReader;
    private final MongoClient client;
    private final Resource resource;
    private final ModificationHelper modificationHelper;

    public DocumentWriter(MongoClient client, DocumentReader documentReader, ModificationHelper modificationHelper) {
        this.client = client;
        this.documentReader = documentReader;
        this.resource = documentReader.getResource();
        this.modificationHelper = modificationHelper;
    }

    @Override
    protected void subscribeActual(Observer<? super DocumentsBatch> observer) {
        AtomicInteger documentCountTracker = new AtomicInteger(0);

        this.documentReader
                .flatMap(new Function<DocumentsBatch, ObservableSource<DocumentsBatch>>() {
                    @Override
                    public ObservableSource<DocumentsBatch> apply(DocumentsBatch batch) throws Exception {
                        // you got entire documents in here
                        // go save them to the target database in parallel
                        // NOTE: Running on the .subscribeOn(Schedulers.io()) threads makes the order quite random.
                        return Observable
                                .just(batch.getDocuments())
                                .observeOn(Schedulers.io())
                                .map(documents -> {
                                    MongoCollection<Document> collection = getMongoCollection();
                                    Document operation = new Document("operation", "insertMany");
                                    MongoDBHelper.performOperationWithRetry(() -> {
                                        InsertManyOptions options = new InsertManyOptions();
                                        options.ordered(false);
                                        try {
                                            collection.insertMany(documents, options);
                                        }
                                        catch (Exception e) {
                                            // do nothing
                                        }
                                        return documents.size();
                                    }, operation);

                                    String message = String.format("Batch %s. Inserted %d documents into target collection: %s",
                                            batch.getBatchId(), documents.size(), resource.getNamespace());
                                    logger.info(message);
                                    documentCountTracker.addAndGet(documents.size());

                                    observer.onNext(batch);
                                    documentReader.releaseThrottler();
                                    return batch;
                                });
                    }
                })
                .subscribe(
                    batch -> observer.onNext(batch),
                    err -> observer.onError(err),
                    () -> {
                        logger.info("Completed writing {} documents to Resource: {}", documentCountTracker.get(), this.resource);
                        observer.onComplete();
                    }
                );
    }

    private MongoCollection<Document> getMongoCollection() {
    	// added support for renaming
        Resource mappedResource = modificationHelper.getMappedResource(resource);
    	String namespaceName = mappedResource.getNamespace();
    	if (resource.isEntireDatabase()) {
    		namespaceName = resource.getDatabase();
    	}
        return BaseDocumentWriter.getInstance(client).getMongoCollection(
                namespaceName,
                mappedResource.getDatabase(),
                mappedResource.getCollection());
    }


}
