package com.mongodb.migratecluster.oplog;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * File: OplogReader
 * Author: Shyam Arjarapu
 * Date: 1/14/19 7:15 AM
 * Description:
 *
 * A class to help read the oplog entries and publish them for any subscribers
 *
 */
public class OplogReader extends Observable<Document> {
    private final MongoClient client;
    private final BsonTimestamp lastTimeStamp;

    final static Logger logger = LoggerFactory.getLogger(OplogReader.class);

    public OplogReader(MongoClient client, BsonTimestamp lastTimeStamp) {
        this.client = client;
        this.lastTimeStamp = lastTimeStamp;

        this.client.setReadPreference(ReadPreference.secondaryPreferred());
    }

    @Override
    protected void subscribeActual(Observer<? super Document> observer) {
        MongoCollection<Document> collection =
            MongoDBHelper.getCollection(client, "local", "oplog.rs");

        Document query = getFindQuery();
        MongoCursor<Document> cursor =
            collection
                .find(query)
                .sort(new Document("$natural", 1))
                .cursorType(CursorType.Tailable)
                .cursorType(CursorType.TailableAwait)
                .noCursorTimeout(true)
                .iterator();

        while (cursor.hasNext()){
            Document document = cursor.next();
            logger.info("Reading a document with ts {}", document.get("ts"));
            observer.onNext(document);
        }
    }

    private Document getFindQuery() {
        Document noOpFilter = new Document("op", new Document("$ne", "n"));
        if (lastTimeStamp == null) {
            return noOpFilter;
        }
        else{
            Document timestampFilter = new Document("ts", new Document("$gt", lastTimeStamp));
            List<Document> filters = new ArrayList<>();
            filters.add(noOpFilter);
            filters.add(timestampFilter);

            return new Document("$and", filters);
        }
    }
}
