package com.mongodb.migratecluster.oplog;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * File: OplogReader
 * Author: shyam.arjarapu
 * Date: 4/29/17 4:46 PM
 * Description:
 */
public class OplogReader extends Observable<Document> {
    private final MongoClient client;
    private final BsonTimestamp lastTimeStamp;

    public OplogReader(MongoClient client, BsonTimestamp lastTimeStamp) {
        this.client = client;
        this.lastTimeStamp = lastTimeStamp;
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
            observer.onNext(document);
        }
    }

    private Document getFindQuery() {
        // TODO: just reads one time but doesn't till to the end
        Document noopFilter = new Document("op", new Document("$ne", "n"));
        if (lastTimeStamp == null) {
            return noopFilter;
        }
        else{
            List<Document> filters = new ArrayList<>();
            filters.add(noopFilter);

            Document timestampFilter = new Document("ts", new Document("$gt", lastTimeStamp));
            filters.add(timestampFilter);

            return new Document("$and", filters);
        }
    }
}
