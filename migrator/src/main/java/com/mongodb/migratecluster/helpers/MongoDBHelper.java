package com.mongodb.migratecluster.helpers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * File: MongoDBHelper
 * Author: shyam.arjarapu
 * Date: 4/29/17 10:08 PM
 * Description:
 */
public  class MongoDBHelper {

    public static MongoDatabase getDatabase(MongoClient client, String databaseName) {
       MongoDatabase database = client.getDatabase(databaseName);
        return database;
    }

    public static MongoCollection<Document> getCollection(MongoClient client, String databaseName, String collectionName) {
        MongoDatabase database = getDatabase(client, databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection;
    }

}
