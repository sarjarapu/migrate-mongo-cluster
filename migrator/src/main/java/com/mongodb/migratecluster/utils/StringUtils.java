package com.mongodb.migratecluster.utils;

/**
 * File: StringUtils
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:48 PM
 * Description:
 */
public class StringUtils {
    public static String valueOrDefault(String value) {
        if (value == null) {
            return "";
        }
        return value;
    }
}
