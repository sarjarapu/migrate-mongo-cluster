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

    @Test
    public void anyOnNumberGreaterThanXReturnsCorrectValues() {
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(3);
        items.add(5);
        items.add(7);
        items.add(9);
        items.add(11);

        boolean hasGreaterThan10 = ListUtils.any(items, i -> i > 10);
        Assert.assertEquals(hasGreaterThan10, true);

        boolean hasGreaterThanEqual11 = ListUtils.any(items, i -> i >= 11);
        Assert.assertEquals(hasGreaterThanEqual11, true);

        boolean hasGreaterThan11 = ListUtils.any(items, i -> i > 11);
        Assert.assertEquals(hasGreaterThan11, false);
    }

    @Test
    public void joinOnZeroNumbersReturnsEmptyString() {
        List<Integer> items = new ArrayList<>();

        String value = ListUtils.join(items, ',');
        Assert.assertEquals(value, "");
    }

    @Test
    public void joinOnOneNumberReturnsEmptyString() {
        List<Integer> items = new ArrayList<>();
        items.add(1);

        String value = ListUtils.join(items, ',');
        Assert.assertEquals(value, "1");
    }

    @Test
    public void joinOnNumbersReturnsCorrectValues() {
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(3);
        items.add(5);
        items.add(7);
        items.add(9);
        items.add(11);

        String value = ListUtils.join(items, ',');
        Assert.assertEquals(value, "1,3,5,7,9,11");
    }
}
