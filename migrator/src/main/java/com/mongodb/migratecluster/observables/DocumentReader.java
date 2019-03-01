package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.model.DocumentsBatch;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * File: DocumentReader
 * Author: shyam.arjarapu
 * Date: 4/26/17 4:48 AM
 * Description:
 * this class helps you read full documents in batches
 * and publish them for any subscribers to listen.
 */
public class DocumentReader extends Observable<DocumentsBatch> {
    final static Logger logger = LoggerFactory.getLogger(DocumentReader.class);
    private final Resource resource;
    private final Document readFromDocumentId;
    private  MongoCollection<Document> collection;
    private final int BATCH_SIZE_DOC_READER = 5; //1000


    public DocumentReader(MongoClient client, Resource resource, Document readFromDocumentId) {
        this.resource = resource;
        this.readFromDocumentId = readFromDocumentId;
        this.collection = client.getDatabase(resource.getDatabase()).getCollection(resource.getCollection());
    }

    /**
     * @param observer
     */
    @Override
    protected void subscribeActual(Observer<? super DocumentsBatch> observer) {
        Observable<Object> observable = new DocumentIdReader(collection, resource, readFromDocumentId);
        AtomicInteger docsCount = new AtomicInteger(0);
        AtomicInteger batchIdTracker = new AtomicInteger(0);

        // fetch the ids and do bulk read of 1000 docs at a time
        observable
                .subscribeOn(Schedulers.io())
                .buffer(BATCH_SIZE_DOC_READER)
                .observeOn(Schedulers.io())
                .flatMap(new Function<List<Object>, Observable<DocumentsBatch>>() {
                    @Override
                    public Observable<DocumentsBatch> apply(List<Object> ids) throws Exception {
                        return new DocumentsObservable(collection, getResource(), batchIdTracker.getAndAdd(1), ids.toArray());
                    }
                })
                .map(batch -> {
                    logger.info("reader for resource: {} got {} documents; so far read total {} documents in this run.",
                            this.resource.getNamespace(),  batch.getSize(), docsCount.addAndGet(batch.getSize()));
                    return batch;
                })
                .subscribeWith(observer);

//                .subscribe(batch -> {
//                    logger.info("reader for resource: {} got {} documents; so far read total {} documents in this run.",
//                            this.resource.getNamespace(),  batch.getSize(), docsCount.addAndGet(batch.getSize()));
//                    observer.onNext(batch);
//                }, err -> {
//                    observer.onError(err);
//                }, () -> {
//                    observer.onComplete();
//                });

        // NOTE: by not blocking here, there is possibility of missing last set in the buffer
        // observable.blockingLast();
        // observer.onComplete();
        logger.info("reader for resource: {} completed. total documents read: {}",
                this.resource.getNamespace(),  docsCount);
    }

    public Resource getResource() {
        return resource;
    }
}