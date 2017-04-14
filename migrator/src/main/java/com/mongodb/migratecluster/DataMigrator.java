package com.mongodb.migratecluster;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.*;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shyamarjarapu on 4/13/17.
 */
public class DataMigrator {
    final static Logger logger = LoggerFactory.getLogger(DataMigrator.class);
    private ApplicationOptions appOptions;

    public DataMigrator(ApplicationOptions appOptions) {
        this.appOptions = appOptions;
    }

    private boolean isValidOptions() {
        // on appOptions source, target, oplog must all be present
        if (
                (this.appOptions.getSourceCluster() == "") ||
                (this.appOptions.getTargetCluster() == "") ||
                (this.appOptions.getOplogStore() == "")
            ) {
            // invalid input
            return false;
        }
        return true;
    }

    public void process() throws AppException {
        // check if the appOptions are valid
        if (!this.isValidOptions()) {
            String message = String.format("invalid input args for sourceCluster, targetCluster and oplog. \ngiven: %s", this.appOptions.toString());
            throw new AppException(message);
        }

        // loop through source and copy to target
        readSourceClusterConfigDatabase();
    }

    private void readSourceClusterConfigDatabase() {
        // Use a Connection String
        String connectionString = String.format("mongodb://%s", this.appOptions.getSourceCluster());
        MongoClientURI uri = new MongoClientURI(connectionString);
        MongoClient client = new MongoClient(uri);

        listDatabases(client);
        listCollections(client, "config");

        MongoDatabase database = client.getDatabase("social");
        listDocuments(database, "user");

        client.close();
    }

    private void listDatabases(MongoClient client) {
        logger.debug("");
        ListDatabasesIterable<Document> databases = client.listDatabases();
        MongoCursor<Document> dbIterator = databases.iterator();
        while(dbIterator.hasNext()) {
            Document document = dbIterator.next();
            logger.debug(document.toJson());
        }
    }

    private void listCollections(MongoClient client, String databaseName) {
        logger.debug("");
        MongoDatabase database = client.getDatabase(databaseName);
        ListCollectionsIterable<Document> collections = database.listCollections();
        MongoCursor<Document> collIterator = collections.iterator();
        while(collIterator.hasNext()) {
            Document document = collIterator.next();
            logger.debug(document.toJson());
        }
    }

    private void listDocuments(MongoDatabase database, String collectionName) {
        logger.debug("");
        MongoCollection<Document> collection = database.getCollection(collectionName);
        // TODO: filter and sort by _id
        MongoCursor<Document> iterator = collection.find().iterator();
        while(iterator.hasNext()) {
            Document document = iterator.next();
            logger.debug(document.toJson());
        }
    }
}
