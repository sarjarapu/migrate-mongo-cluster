package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.utils.ListUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class DatabaseMigrator {
    final static Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class);
    private ApplicationOptions appOptions;
    private final MongoClient client;
    private final Document databaseDocument;

    public DatabaseMigrator(MongoClient client, Document databaseDocument) {
        this.client = client;
        this.databaseDocument = databaseDocument;
    }


    public void migrate(ApplicationOptions appOptions) throws AppException {
        String databaseName = this.databaseDocument.getString("name");

        MongoDatabase database = client.getDatabase(databaseName);
        List<Document> list = IteratorHelper.getCollections(database);

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

        //List<Document> filteredList = getCollectionForProcessing(appOptions);
        List<CollectionMigrator> migrators = ListUtils.select(list,
                item -> {
                    MongoCollection<Document> collection = database.getCollection(item.getString("name"));
                    return new CollectionMigrator(collection);
                });
        migrators.forEach(d -> {
            try {
                d.migrate(appOptions);
            } catch (AppException e) {
                e.printStackTrace();
            }
        });
    }

    private List<Document> getCollectionForProcessing(ApplicationOptions appOptions) {
        return null;
    }
}
