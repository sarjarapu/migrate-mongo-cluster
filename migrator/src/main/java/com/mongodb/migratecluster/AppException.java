package com.mongodb.migratecluster;

import java.io.IOException;

/**
 * Created by shyamarjarapu on 4/13/17.
 */
public class AppException extends Exception {
    public AppException(String message) {
        super(message);
    }

    public AppException(String message, Exception e) {
        super(message, e);
    }
}
