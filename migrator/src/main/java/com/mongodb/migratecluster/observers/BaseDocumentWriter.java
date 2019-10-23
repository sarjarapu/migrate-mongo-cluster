package com.mongodb.migratecluster.observers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * File: migrator
 * Author: shyamarjarapu
 * Date: 4/18/17 2:20 PM
 * Description:
 */
public class BaseDocumentWriter {

    protected final MongoClient client;
    protected final ConcurrentMap<String, MongoDatabase> mongoDatabaseMap;
    protected final ConcurrentMap<String, MongoCollection<Document>> mongoCollectionMap;

    private static ConcurrentMap<MongoClient, BaseDocumentWriter> instance = new ConcurrentHashMap<>();
    public static BaseDocumentWriter getInstance(MongoClient client) {
        BaseDocumentWriter value = instance.get(client);
        if (value == null) {
            value = new BaseDocumentWriter(client);
            instance.put(client, value);
        }
        return  value;
    }

    public BaseDocumentWriter(MongoClient client) {
        this.client = client;
        this.mongoCollectionMap = new ConcurrentHashMap<>();
        this.mongoDatabaseMap = new ConcurrentHashMap<>();
    }

    protected MongoDatabase getMongoDatabase(String dbName) {
        if (mongoDatabaseMap.containsKey(dbName)) {
            return mongoDatabaseMap.get(dbName);
        }

        MongoDatabase database = this.client.getDatabase(dbName);
        mongoDatabaseMap.put(dbName, database);
        return database;
    }

    public MongoCollection<Document> getMongoCollection(String namespace, String databaseName, String collectionName) {
        if (mongoCollectionMap.containsKey(namespace)) {
            return mongoCollectionMap.get(namespace);
        }

        MongoDatabase database = getMongoDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        mongoCollectionMap.put(namespace, collection);
        return collection;
    }

    protected CreateCollectionOptions getCreateCollectionOptions(Document document) {
        CreateCollectionOptions collectionOptions = new CreateCollectionOptions();

        if (document.containsKey("capped")) {
            collectionOptions.capped(document.getBoolean("capped"));
        }
        if (document.containsKey("maxDocuments")) {
            collectionOptions.maxDocuments(document.getLong("maxDocuments"));
        }
        if (document.containsKey("sizeInBytes")) {
            collectionOptions.sizeInBytes(document.getLong("sizeInBytes"));
        }
        return collectionOptions;
    }
}
