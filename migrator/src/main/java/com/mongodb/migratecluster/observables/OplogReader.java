package com.mongodb.migratecluster.observables;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;

import java.util.Date;

/**
 * File: OplogReader
 * Author: shyam.arjarapu
 * Date: 4/29/17 4:46 PM
 * Description:
 */
public class OplogReader extends Observable<Document> {
    private final MongoClient client;
    private final Date lastTimeStamp;

    public OplogReader(MongoClient client, Date lastTimeStamp) {
        this.client = client;
        this.lastTimeStamp = lastTimeStamp;
    }

    @Override
    protected void subscribeActual(Observer<? super Document> observer) {
        MongoCollection<Document> collection =
                MongoDBHelper.getCollection(client, "local", "oplog.rs");

        Document timeQuery = getTimeQuery(this.lastTimeStamp);
        MongoCursor<Document> cursor =
                collection
                        .find(timeQuery)
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

    private Document getTimeQuery(Date lastTimeStamp) {
        return (lastTimeStamp == null)
                ? new Document()
                : new Document("ts", new Document("$gt",lastTimeStamp));
    }
}
