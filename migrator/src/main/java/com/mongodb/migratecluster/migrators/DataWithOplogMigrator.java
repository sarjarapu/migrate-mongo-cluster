package com.mongodb.migratecluster.migrators;

import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;

public class DataWithOplogMigrator extends BaseMigrator {

    private final DataMigrator dataMigrator;
    //private final OplogMigrator oplogMigrator;

    public DataWithOplogMigrator(ApplicationOptions options) {
        super(options);

        dataMigrator = new DataMigrator(options);
        //oplogMigrator = new OplogMigrator(options);
    }

    @Override
    public void process() throws AppException {
        // TODO:
        // if not already exists track the oplog / shard
        // move all the data from source to target
        // once that is complete apply oplog entries
        dataMigrator.process();
        //oplogMigrator.process();
    }

    @Override
    public void preprocess() {
        //oplogMigrator.preprocess();
        dataMigrator.preprocess();
    }
}
