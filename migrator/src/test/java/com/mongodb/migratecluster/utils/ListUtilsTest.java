package com.mongodb.migratecluster.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class ListUtilsTest {

    @Test
    public void whereFilterOnNumberReturnsCorrectValues() {
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(3);
        items.add(5);
        items.add(7);
        items.add(9);
        items.add(11);

        List<Integer> filteredList = ListUtils.where(items, i -> i <= 5);
        Assert.assertNotEquals(filteredList, null);
        Assert.assertEquals(filteredList.size(), 3);
        Assert.assertArrayEquals(filteredList.toArray(), new Integer[] {1,3,5});
    }

    @Test
    public void selectOnNumberPlusOneReturnsCorrectValues() {
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(3);
        items.add(5);
        items.add(7);
        items.add(9);
        items.add(11);

        List<Integer> filteredList = ListUtils.select(items, i -> i + 1);
        Assert.assertNotEquals(filteredList, null);
        Assert.assertEquals(filteredList.size(), 6);
        Assert.assertArrayEquals(filteredList.toArray(), new Integer[] {2,4,6,8,10,12});
    }
}
