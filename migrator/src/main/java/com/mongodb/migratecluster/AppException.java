package com.mongodb.migratecluster;

/**
 * File: AppException
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:49 PM
 * Description:
 */
public class AppException extends Exception {
    public AppException(String message) {
        super(message);
    }

    // https://github.com/tmc/mongologtools
    // https://github.com/ParsePlatform/logtailer
    public AppException(String message, Exception e) {
        super(message, e);
    }
}
