package com.mongodb.migratecluster;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shyamarjarapu on 4/13/17.
 */
public class InputArgsParser {
    final static Logger logger = LoggerFactory.getLogger(InputArgsParser.class);

    Options _options;
    Boolean _optionsLoaded = false;

    public InputArgsParser() {
        _options = new Options();
    }

    public void loadOptions() {
        if (!_optionsLoaded) {

            _options.addOption("h", "help", false, "print this message");
            _options.addOption("c", "config", true,"configuration file for migration");

            _optionsLoaded = true;
        }
    }

    public CommandLine parse(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(_options, args);
        } catch (ParseException e) {
            logger.error("Error while parsing the input arguments. Refer to usage", e);
            printHelp();
        }
        return cmd;
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("migratecluster",  _options);
    }
}
