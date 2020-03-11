package com.mongodb.migratecluster;


import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ApplicationOptionsLoader;
import com.mongodb.migratecluster.commandline.InputArgsParser;
import com.mongodb.migratecluster.migrators.BaseMigrator;
import com.mongodb.migratecluster.migrators.DataWithOplogMigrator;
import com.mongodb.migratecluster.migrators.OplogMigrator;
import com.mongodb.migratecluster.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * File: Application
 * Author: Shyam Arjarapu
 * Date: 1/12/17 9:40 AM
 * Description:
 *
 * A class to run the migration of MongoDB cluster
 * based on the inputs configured in config file
 *
 */
public class Application {
    private final static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Application application = new Application();
        application.run(args);
    }

    /**
     * Run's the application migration process using the migrator
     *
     * @param args command line arguments
     */
    private void run(String[] args) {
    	logger.info("Runtime -> Java: "+System.getProperty("java.vendor")+ " " + System.getProperty("java.version") + " OS: "+System.getProperty("os.name")+" " +System.getProperty("os.version"));
        ApplicationOptions options = getApplicationOptions(args);

        BaseMigrator migrator = null;
        if (options.isOplogOnly())
        	migrator = new OplogMigrator(options);
        else
        	migrator = new DataWithOplogMigrator(options);
        try {
        	migrator.preprocess();	
            migrator.process();
        } catch (AppException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Get's the application options injected into the command
     * line arguments or interprets the configuration file options
     *
     * @param args command line arguments
     * @return application options object representing the options to be used for migration
     * @see ApplicationOptions
     */
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
                System.exit(1);
            }
        }

        logger.info("Application Options: {}", appOptions.toString());
        if (appOptions.getBlackListFilter().size() > 0 && appOptions.getWhiteListFilter().size() > 0) {
        	logger.error("Can't specify BlackList and WhiteList" );
        	System.exit(1);
        }
        if (appOptions.getWhiteListFilter().size() > 0 && !appOptions.isOplogOnly()) {
        	logger.error("WhiteList in use, oplogOnly must be selected.");
        	System.exit(1);
        }
        if(appOptions.isDropTarget() && appOptions.isOplogOnly()) {
        	logger.error("oplogOnly can't be used if the target is dropped.");
        	System.exit(1);
        }
        return appOptions;
    }

}
