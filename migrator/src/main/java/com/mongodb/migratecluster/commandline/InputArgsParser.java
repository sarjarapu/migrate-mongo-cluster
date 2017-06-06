package com.mongodb.migratecluster.commandline;

import com.mongodb.migratecluster.utils.StringUtils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File: InputArgsParser
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:47 PM
 * Description:
 */
public class InputArgsParser {
    final static Logger logger = LoggerFactory.getLogger(InputArgsParser.class);

    Options options;
    Boolean areOptionsSet = false;

    public InputArgsParser() {
        options = new Options();
    }

    public void loadSupportedOptions() {
        if (!areOptionsSet) {

            options.addOption("h", "help", false, "print this message");
            options.addOption("c", "config", true,"configuration file for migration");
            options.addOption("s", "source", true,"source cluster connection string");
            options.addOption("t", "target", true,"target cluster connection string");
            options.addOption("o", "oplog", false,"oplogStore connection string. only required if oplog headroom is small");
            options.addOption("d", "drop", false,"drop target collections before copying");

            areOptionsSet = true;
        }
    }

    public CommandLine parse(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        cmd = parser.parse(options, args);
        return cmd;
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("migratecluster", options, true);
    }

    public ApplicationOptions getApplicationOptions(String[] args) {
        this.loadSupportedOptions();
        ApplicationOptions appOptions = new ApplicationOptions();
        CommandLine cmd = null;

        // if requested for help or has exceptions while parsing, show help.
        try {
            cmd = this.parse(args);
        } catch (ParseException e) {
            appOptions.setShowHelp(true);
            return appOptions;

        }
        if (cmd.hasOption("h")) {
            appOptions.setShowHelp(true);
            return appOptions;
        }


        // set parsed input values to corresponding properties
        if (cmd.hasOption("c")) {
            appOptions.setConfigFilePath(StringUtils.valueOrDefault(cmd.getOptionValue("c")));
        }
        if (cmd.hasOption("s")) {
            appOptions.setSourceCluster(StringUtils.valueOrDefault(cmd.getOptionValue("s")));
        }
        if (cmd.hasOption("t")) {
            appOptions.setTargetCluster(StringUtils.valueOrDefault(cmd.getOptionValue("t")));
        }
        if (cmd.hasOption("o")) {
            appOptions.setOplogStore(StringUtils.valueOrDefault(cmd.getOptionValue("o")));
        }
        if (cmd.hasOption("d")) {
            appOptions.setDropTarget(true);
        }
        return appOptions;
    }
}
