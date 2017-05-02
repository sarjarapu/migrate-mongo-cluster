package com.mongodb.migratecluster.observables;

import com.mongodb.MongoClient;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;

import java.util.Date;

/**
 * File: OplogWriter
 * Author: shyam.arjarapu
 * Date: 4/30/17 12:30 AM
 * Description:
 */
public class OplogWriter extends Observable<Document> {
    private final MongoClient client;
    private final Date lastTimeStamp;

    public OplogWriter(MongoClient client, Date lastTimeStamp) {
        this.client = client;
        this.lastTimeStamp = lastTimeStamp;
    }

    @Override
    protected void subscribeActual(Observer<? super Document> observer) {
        // take and write the oplogs may be even do timed
        // buffer and save to target oplog collection you have
        // to identify the source server for continuation
    }
}
