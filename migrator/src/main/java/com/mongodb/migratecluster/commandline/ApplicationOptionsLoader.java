package com.mongodb.migratecluster.commandline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.migratecluster.AppException;

import java.io.File;
import java.io.IOException;

/**
 * Created by shyamarjarapu on 4/13/17.
 */
public class ApplicationOptionsLoader {

    public static ApplicationOptions load(String configFilePath) throws AppException {
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse
        ApplicationOptions appOptions = null;
        try {

            File file = new File(configFilePath);
            if (file.exists()) {
                appOptions = mapper.readValue(file, ApplicationOptions.class);
            }
            else {
                String message = String.format("configFilePath: '%s' does not exists", configFilePath);
                throw new AppException(message);
            }

        } catch (IOException e) {
            String message = String.format("error while reading configFilePath: '%s'. exception: %s", configFilePath, e.getMessage());
            throw new AppException(message, e);
        }
        appOptions.setConfigFilePath(configFilePath);
        return appOptions;
    }
}
