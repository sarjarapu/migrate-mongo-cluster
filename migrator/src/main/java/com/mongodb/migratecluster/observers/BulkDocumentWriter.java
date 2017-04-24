package com.mongodb.migratecluster.observers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.observables.ResourceDocument;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * File: BulkDocumentWriter
 * Author: shyam.arjarapu
 * Date: 4/18/17 2:18 PM
 * Description:
 */
public class BulkDocumentWriter extends BaseDocumentWriter implements Consumer<List<ResourceDocument>> {
    private final static Logger logger = LoggerFactory.getLogger(DocumentWriter.class);

    public BulkDocumentWriter(MongoClient client) {
        super(client);
    }

 /*   @Override
    public void onSubscribe(Disposable disposable) {

    }

    @Override
    public void onNext(List<ResourceDocument> resourceDocuments) {
        logger.info(" .... Writing docs on thread: {}", Thread.currentThread().getId());
        Map<Resource, List<ResourceDocument>> resourceDocumentsMap = resourceDocuments
                .stream()
                .collect(Collectors.groupingBy(ResourceDocument::getResource));

        resourceDocumentsMap.forEach((resource, documentList) -> {
            writeDocuments(documentList);
            String message = String.format(" ..... insertMany %d documents into namespace: \"%s\"",
                    documentList.size(),
                    resource.getNamespace());
            logger.info(message);
        });

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }
*/

    private void writeDocuments(List<ResourceDocument> resourceDocuments) {
        if (resourceDocuments == null || resourceDocuments.size() == 0) {
            return;
        }

        Resource resource = resourceDocuments.get(0).getResource();
        MongoCollection<Document> collection =
                getMongoCollection(
                        resource.getNamespace(),
                        resource.getDatabase(),
                        resource.getCollection());

        List<Document> documents =
                resourceDocuments
                        .stream()
                        .map(rd -> rd.getDocument())
                        .collect(Collectors.toList());

        collection.insertMany(documents);
    }

    @Override
    public void accept(List<ResourceDocument> resourceDocuments) throws Exception {
        String message = String.format(" .... Writing docs on thread: %s",
                Thread.currentThread().getId());
        logger.info(message);

        Map<Resource, List<ResourceDocument>> resourceDocumentsMap = resourceDocuments
                .stream()
                .collect(Collectors.groupingBy(ResourceDocument::getResource));

        resourceDocumentsMap.forEach((resource, documentList) -> {
            writeDocuments(documentList);
            String message1 = String.format(" ..... insertMany %d documents into namespace: \"%s\"",
                    documentList.size(),
                    resource.getNamespace());
            logger.info(message1);
        });
    }
}
