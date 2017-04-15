package com.mongodb.migratecluster.migrators;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
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
    private final MongoCollection<Document> collection;

    public CollectionMigrator(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    public void migrate(ApplicationOptions appOptions) throws AppException {
        List<Document> list = IteratorHelper.getDocuments(this.collection);

        if (list == null) {
            String message = "looks like migrate process was invoked before loading the collections";
            logger.warn(message);
            throw new AppException(message);
        }
        else if (list.size() == 0) {
            String message = "looks like no collections found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        list.forEach(item -> logger.debug(item.toJson()));
    }
}
