package com.mongodb.migratecluster;


import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ApplicationOptionsLoader;
import com.mongodb.migratecluster.commandline.InputArgsParser;
import com.mongodb.migratecluster.migrators.DataMigrator;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * File: App
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:49 PM
 * Description:
 */
public class App {
    private final static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        App app = new App();
        app.run(args);
        //app.testFlowable();
    }


    private void testFlowable() {
        // you got _id covered index scanner running here.
        // each id is buffered into bucket of size 100
        // each bucket is run in different thread
        // each thread will read document $in (ids in bucket)


        // working code from blog
        // http://tomstechnicalblog.blogspot.ca/2015/11/rxjava-achieving-parallelization.html
        Observable<Integer> vals = Observable.range(1,1000);
        // flatMap: transform the items emitted by an Observable into Observables,
        // then flatten the emissions from those into a single Observable
        vals.buffer(100)
            .flatMap(new Function<List<Integer>, ObservableSource<List<Integer>>>() {
                @Override
                public ObservableSource<List<Integer>> apply(List<Integer> integers) throws Exception {
                    return Observable
                            .just(integers)
                            .subscribeOn(Schedulers.io())
                            .map(l -> {
                                String message = String.format("Working\t%s\t%s\t%s",
                                        Thread.currentThread().getId(), l.toString(), new Date().getTime());
                                System.out.println(message);
                                Thread.sleep(randInt(0,100));
                                return l;
                            });
                }
            })
            .subscribe(new Consumer<List<Integer>>() {
                @Override
                public void accept(List<Integer> integers) throws Exception {
                    String message = String.format("Received\t%s\t%s\t%s",
                            Thread.currentThread().getId(), integers.toString(), new Date().getTime());
                    System.out.println(message);
                }
            });
        waitSleep(10000);
    }
    public static void waitSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private static final Random rand = new Random();
    public static int randInt(int min, int max) {
        return rand.nextInt((max - min) + 1) + min;
    }


    private void run(String[] args){
        ApplicationOptions options = getApplicationOptions(args);
        DataMigrator migrator = new DataMigrator(options);
        try {
            migrator.process();
        } catch (AppException e) {
            logger.error(e.getMessage());
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private ApplicationOptions getApplicationOptions(String[] args) {
        logger.debug("Parsing the command line input args");
        InputArgsParser parser = new InputArgsParser();
        ApplicationOptions appOptions = parser.getApplicationOptions(args);

        if (appOptions.isShowHelp()) {
            parser.printHelp();
            System.exit(0);
        }

        String configFilePath = appOptions.getConfigFilePath();
        if (configFilePath != "") {
            try {
                logger.debug("configFilePath is set to {}. overriding command line input args if applicable", configFilePath);
                appOptions = ApplicationOptionsLoader.load(configFilePath);
            } catch (AppException e) {
                logger.error(e.getMessage());
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

        logger.info("Application Options: {}", appOptions.toString());
        return appOptions;
    }

}
