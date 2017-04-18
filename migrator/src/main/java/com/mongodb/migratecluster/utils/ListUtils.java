package com.mongodb.migratecluster.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * File: ListUtils
 * Author: shyam.arjarapu
 * Date: 4/14/17 11:48 PM
 * Description:
 */
public class ListUtils {

    public static <T> List<T> where(List<T> list, Predicate<T> predicate) {
        List<T> filteredList = new ArrayList<>();
        for (T item : list) {
            if (predicate.test(item)) {
                filteredList.add(item);
            }
        }
        return filteredList;
    }

    public static <T,R> List<R> select(List<T> list, Function<T, R> function) {
        List<R> filteredList = new ArrayList<>();
        for (T item : list) {
            filteredList.add(function.apply(item));
        }
        return filteredList;
    }

    public static <T> boolean any(List<T> list, Predicate<T> predicate) {
        for (T item : list) {
            if (predicate.test(item)) {
                return true;
            }
        }
        return false;
    }

    public static <T> String join(List<T> list, char separator) {
        StringBuilder sb = new StringBuilder();
        for (T item : list) {
            sb.append(separator);
            sb.append(item.toString());
        }
        return (sb.length() > 0)
                    ? sb.substring(1)
                    : "";
    }
}
