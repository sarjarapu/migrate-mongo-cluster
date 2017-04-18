package com.mongodb.migratecluster.observables;

import io.reactivex.Observable;
import io.reactivex.Observer;
import org.bson.Document;

/**
 * Created by shyamarjarapu on 4/17/17.
 */
public class CollectionObservable extends Observable<DocumentObservable> {
    private Observable<DocumentObservable> documentObservables;

    public CollectionObservable(Observable<DocumentObservable> documentObservables) {
        this.documentObservables = documentObservables;
    }

    @Override
    protected void subscribeActual(Observer<? super DocumentObservable> observer) {
        /*this.documentObservables.subscribe(d -> d.next)
        while (iterator.hasNext()) {
            Document item = iterator.next();
            if (!item.isEmpty()) {
                observer.onNext(item);
            }
        }
        observer.onComplete();*/
    }
}
