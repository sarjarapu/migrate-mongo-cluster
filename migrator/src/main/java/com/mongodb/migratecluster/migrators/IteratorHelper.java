package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.utils.ListUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * File: IteratorHelper
 * Author: shyam.arjarapu
 * Date: 4/14/17 11:43 PM
 * Description:
 */
public class IteratorHelper {
    private final static Logger logger = LoggerFactory.getLogger(IteratorHelper.class);

    public static MongoCollection<Document> getMongoCollection(MongoClient client, Resource resource) {
        MongoDatabase database = client.getDatabase(resource.getDatabase());
        return database.getCollection(resource.getCollection());
    }

    public static Map<String, List<Resource>> getSourceResources(MongoClient client) {
        Map<String, List<Resource>> dictionary = new HashMap<>();

        List<Document> databases = getDatabases(client);
       /* databases.forEach(d -> {
            String databaseName = d.getString("name");
            MongoDatabase database = client.getDatabase(databaseName);
            List<Resource> resources = ListUtils.select(getCollections(database),
                    c -> new Resource(databaseName, c));
            dictionary.put(databaseName, resources);
        });*/
        return dictionary;
    }

    public static List<Document> getDatabases(MongoClient client) {
        List<Document> list = new ArrayList<>();

        ListDatabasesIterable<Document> cursor = client.listDatabases();
        MongoCursor<Document> iterator = cursor.iterator();

        while(iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                String message = String.format(" found database name: %s, sizeOnDisk: %s",
                        item.getString("name"), item.get("sizeOnDisk"));
                logger.info(message);
                logger.debug(item.toJson());
                list.add(item);
            }
        }
        return list;
    }


    public static List<Document> getCollections(MongoDatabase database) {
        List<Document> list = new ArrayList<>();

        ListCollectionsIterable<Document> cursor = database.listCollections();
        MongoCursor<Document> iterator = cursor.iterator();

        while(iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                logger.info(" ... found collection: {}.{}", database.getName(), item.getString("name"));
                logger.debug(item.toJson());
                list.add(item);
            }
        }
        return list;
    }
}
