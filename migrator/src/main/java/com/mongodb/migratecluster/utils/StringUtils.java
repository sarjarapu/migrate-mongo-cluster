package com.mongodb.migratecluster.utils;

/**
 * Created by shyamarjarapu on 4/13/17.
 */
public class StringUtils {
    public static String valueOrDefault(String value) {
        if (value == null) {
            return "";
        }
        return value;
    }
}
