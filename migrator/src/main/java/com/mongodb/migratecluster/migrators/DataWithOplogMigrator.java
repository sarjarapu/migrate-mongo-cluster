package com.mongodb.migratecluster.migrators;

import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;

/**
 * File: DataWithOplogMigrator
 * Author: Shyam Arjarapu
 * Date: 1/12/19 9:50 AM
 * Description:
 *
 * A class to help migrate collection data and oplogs from source to target
 */
public class DataWithOplogMigrator extends BaseMigrator {

//    private final CollectionDataMigrator dataMigrator;
    private final OplogMigrator oplogMigrator;

    public DataWithOplogMigrator(ApplicationOptions options) {
        super(options);

//        dataMigrator = new CollectionDataMigrator(options);
        oplogMigrator = new OplogMigrator(options);
    }

    /**
     * A method that is invoked before the actual migration process
     */
    @Override
    public void preprocess() {
        oplogMigrator.preprocess();
//        dataMigrator.preprocess();
    }

    /**
     *
     * A process method that implements actual migration
     * of collection data and oplog store from source to target.
     *
     * @throws AppException
     * @see AppException
     */
    @Override
    public void process() throws AppException {
        // TODO:
        // if not already exists track the oplog / shard
        // move all the data from source to target
        // once that is complete apply oplog entries
//        dataMigrator.process();
        oplogMigrator.process();
    }
}
