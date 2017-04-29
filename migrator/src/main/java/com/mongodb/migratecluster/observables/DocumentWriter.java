package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observers.BaseDocumentWriter;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Observer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * File: DocumentWriter
 * Author: shyam.arjarapu
 * Date: 4/26/17 5:02 AM
 * Description:
 */
public class DocumentWriter extends Observable<List<ResourceDocument>> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentWriter.class);
    private final DocumentReader documentReader;
    private final MongoClient client;
    private final Resource resource;

    public DocumentWriter(MongoClient client, DocumentReader documentReader) {
        this.client = client;
        this.documentReader = documentReader;
        this.resource = documentReader.getResource();
    }

    @Override
    protected void subscribeActual(Observer<? super List<ResourceDocument>> observer) {
        AtomicInteger atomicInteger = new AtomicInteger(0);

        Observable<List<ResourceDocument>> observable = this.documentReader
            .flatMap(new Function<List<ResourceDocument>, ObservableSource<List<ResourceDocument>>>() {
                @Override
                public ObservableSource<List<ResourceDocument>> apply(List<ResourceDocument> documents) throws Exception {
                    // you got entire documents in here
                    // go save them to the target database in parallel
                    return Observable
                            .just(documents)
                            .subscribeOn(Schedulers.io())
                            .map(rdocs -> {
                                MongoCollection<Document> collection = getMongoCollection();
                                List<Document> docs = rdocs.stream()
                                        .map(rd -> rd.getDocument())
                                        .collect(Collectors.toList());
                                collection.insertMany(docs);

                                String message = String.format("Inserted %d documents into target collection: %s",
                                        documents.size(), resource.getNamespace());
                                logger.info(message);
                                atomicInteger.addAndGet(documents.size());

                                observer.onNext(rdocs);
                                return rdocs;
                            });
                }
            });

        observable.blockingSubscribe();

        logger.info("Completed writing {} documents to Resource: {}", atomicInteger.get(), this.resource);
        observer.onComplete();
    }

    private MongoCollection<Document> getMongoCollection() {
        return BaseDocumentWriter.getInstance(client).getMongoCollection(
                resource.getNamespace(),
                resource.getDatabase(),
                resource.getCollection());
    }


}
