package com.mongodb.migratecluster.migrators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	final static Logger logger = LoggerFactory.getLogger(OplogMigrator.class);
	
    private final CollectionDataMigrator dataMigrator;
    private final BaseMigrator oplogMigrator;

    public DataWithOplogMigrator(ApplicationOptions options) {
        super(options);

        dataMigrator = new CollectionDataMigrator(options);
        logger.info("Setting up");
        if (options.changestream()) {
        	logger.info("Changestream Migrator");
        	oplogMigrator = new ChangestreamMigrator(options);
        } else {
        	logger.info("Oplog Migrator");
        	oplogMigrator = new OplogMigrator(options);
        }
    }

    /**
     * A method that is invoked before the actual migration process
     */
    @Override
    public void preprocess() {
        oplogMigrator.preprocess();
        dataMigrator.preprocess();
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
        dataMigrator.process();
        oplogMigrator.process();
    }
}
