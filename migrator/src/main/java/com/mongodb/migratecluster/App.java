package com.mongodb.migratecluster;


import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shyam.arjarapu on 4/13/17.
 */
public class App {
    final static Logger logger = LoggerFactory.getLogger(App.class);

    InputArgsParser argsParser;

    public static void main(String[] args) {

        App app = new App();
        app.run(args);
    }

    private void run(String[] args){
        logger.debug("Testing java code");
        loadCommandLineParser(args);
    }

    private void loadCommandLineParser(String[] args) {
        argsParser = new InputArgsParser();
        argsParser.loadOptions();

        CommandLine cmd = argsParser.parse(args);
        if (cmd.hasOption("h")) {
            argsParser.printHelp();
            return ;
        }
    }
}
