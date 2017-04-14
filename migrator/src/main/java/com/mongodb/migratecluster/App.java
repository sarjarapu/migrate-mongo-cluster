package com.mongodb.migratecluster;


import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ApplicationOptionsLoader;
import com.mongodb.migratecluster.commandline.InputArgsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shyam.arjarapu on 4/13/17.
 */
public class App {
    private final static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        App app = new App();
        app.run(args);
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
