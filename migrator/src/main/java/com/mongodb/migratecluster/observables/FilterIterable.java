package com.mongodb.migratecluster.observables;

import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.migrators.DataMigrator;
import io.reactivex.Flowable;
import io.reactivex.functions.Predicate;
import io.reactivex.internal.operators.flowable.FlowableFilter;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * File: FilterIterable
 * Author: shyam.arjarapu
 * Date: 4/26/17 7:11 PM
 * Description:
 */
public class FilterIterable implements Predicate<Document> {
    final static Logger logger = LoggerFactory.getLogger(FilterIterable.class);

    private final Map<String, List<ResourceFilter>> resourceFilters;

    public FilterIterable(List<ResourceFilter> filters) {
        this.resourceFilters = filters
            .stream()
            .collect(Collectors.groupingBy(ResourceFilter::getDatabase));
    }


    @Override
    public boolean test(Document document) throws Exception {
        String databaseName = document.getString("name");
        if (this.resourceFilters.containsKey(databaseName)) {
            List<ResourceFilter> filters = this.resourceFilters.get(databaseName);
            if (filters != null && filters.size() > 0) {
                return filters.get(0).isEntireDatabase();
            }
        }
        return false;
    }
}
