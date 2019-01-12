package com.mongodb.migratecluster;

/**
 * File: AppException
 * Author: Shyam Arjarapu
 * Date: 1/12/19 12:20 AM
 * Description:
 *
 * a class representing the application exception that
 * can be caught and handled for further processing
 *
 */
public class AppException extends Exception {

    /**
     * @param message a string representation of the error message
     */
    public AppException(String message) {
        super(message);
    }

    /**
     * @param message a string representation of the error message
     * @param e an exception
     */
    public AppException(String message, Exception e) {
        super(message, e);
    }
}
