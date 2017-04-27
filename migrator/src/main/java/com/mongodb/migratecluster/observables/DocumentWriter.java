package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observers.BaseDocumentWriter;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Observer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * File: migrator
 * Author: shyamarjarapu
 * Date: 4/26/17 5:02 AM
 * Description:
 */
public class DocumentWriter extends Observable<List<ResourceDocument>> {
    private final static Logger logger = LoggerFactory.getLogger(Observable.class);
    private final DocumentReader documentReader;
    private final MongoClient client;
    private final Resource resource;

    public DocumentWriter(MongoClient client, DocumentReader documentReader, Resource resource) {
        this.documentReader = documentReader;
        this.client = client;
        this.resource = resource;
    }

    @Override
    protected void subscribeActual(Observer<? super List<ResourceDocument>> observer) {
        this.documentReader
                .flatMap(new Function<List<ResourceDocument>, ObservableSource<Integer>>() {
                    @Override
                    public ObservableSource<Integer> apply(List<ResourceDocument> documents) throws Exception {
                        // you got entire documents in here
                        // go save them to the target database in parallel
                        return Observable.just(documents)
                                .subscribeOn(Schedulers.io())
                                .map(rdocs -> {
                                    //TODO: you got to have name here hard code for now
                                    MongoCollection<Document> collection =
                                            BaseDocumentWriter.getInstance(client).getMongoCollection(
                                                    resource.getNamespace(),
                                                    resource.getDatabase(),
                                                    resource.getCollection());
                                    //MongoCollection<Document> collection = targetDatabase.getCollection(resource.getCollection());
                                    String message = String.format(" ... writing to target. docs.size: %s", documents.size());
                                    logger.info(message);

                                    List<Document> docs = rdocs.stream()
                                            .map(rd -> rd.getDocument())
                                            .collect(Collectors.toList());
                                    collection.insertMany(docs);
                                    return docs;
                                })
                                .map(l -> l.size());
                    }
                })
                .subscribe(k -> {
                    logger.info("Done processing {}", k.toString());
                });

        logger.info("Waiting for blockingLast on DocumentWriter");
        this.documentReader.blockingLast();
        logger.info("Completed waiting for blockingLast on DocumentWriter");
    }


}
