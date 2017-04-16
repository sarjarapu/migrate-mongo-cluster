package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.utils.ListUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class CollectionMigrator {
    final static Logger logger = LoggerFactory.getLogger(CollectionMigrator.class);
    private final MongoClient client;
    private final Resource resource;

    public CollectionMigrator(MongoClient client, Resource resource) {
        this.client = client;
        this.resource = resource;
    }

    public String getNamespace() {
        return this.resource.getNamespace();
    }

    public void migrate(ApplicationOptions appOptions) throws AppException {
        MongoCollection<Document> collection = IteratorHelper.getMongoCollection(this.client, this.resource);

        if (collection == null) {
            String message = "looks like no collections found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        List<Document> list = IteratorHelper.getDocuments(collection);
        list.forEach(item -> logger.debug(item.toJson()));
    }
}
