package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.migratecluster.commandline.Resource;
import com.mongodb.migratecluster.utils.ListUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class IteratorHelper {
    final static Logger logger = LoggerFactory.getLogger(IteratorHelper.class);

    public static Map<String, List<Resource>> getSourceResources(MongoClient client) {
        Map<String, List<Resource>> dictionary = new HashMap<>();

        List<Document> databases = getDatabases(client);
        databases.forEach(d -> {
            String databaseName = d.getString("name");
            MongoDatabase database = client.getDatabase(databaseName);
            List<Resource> resources = ListUtils.select(getCollections(database),
                    c -> new Resource(databaseName, c.getString("name")));
            dictionary.put(databaseName, resources);
        });
        return dictionary;
    }

    public static List<Document> getDatabases(MongoClient client) {
        List<Document> list = new ArrayList<>();

        ListDatabasesIterable<Document> cursor = client.listDatabases();
        MongoCursor<Document> iterator = cursor.iterator();

        while(iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                logger.debug("found database: {}", item.toJson());
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
                logger.debug("found collection: {}.{}", database.getName(), item.toJson());
                list.add(item);
            }
        }
        return list;
    }


    public static List<Document> getDocuments(MongoCollection<Document> collection) {
        List<Document> list = new ArrayList<>();

        Bson sortCriteria = Sorts.ascending("_id");
        MongoCursor<Document> iterator = collection.find().sort(sortCriteria).iterator();

        while(iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                logger.debug("found item: {}.{}", collection.getNamespace(), item.toJson());
                list.add(item);
            }
        }
        return list;
    }
}
