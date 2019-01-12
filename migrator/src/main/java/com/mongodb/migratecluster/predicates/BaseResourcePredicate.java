package com.mongodb.migratecluster.predicates;

import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.commandline.ResourceFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * File: BaseResourcePredicate
 * Author: shyam.arjarapu
 * Date: 4/27/17 9:34 PM
 * Description:
 */
abstract class BaseResourcePredicate  {
    private final Map<String, List<ResourceFilter>> blacklistResources;

    BaseResourcePredicate(List<ResourceFilter> filters) {
        this.blacklistResources = filters
            .stream()
            .collect(Collectors.groupingBy(ResourceFilter::getDatabase));
    }

    boolean isDatabaseInBlackList(String databaseName) {
        if (this.blacklistResources.containsKey(databaseName)) {
            List<ResourceFilter> filters = this.blacklistResources.get(databaseName);
            if (filters != null && filters.size() > 0) {
                return filters.get(0).isEntireDatabase();
            }
        }
        return false;
    }

    boolean isResourceInBlackList(Resource resource) {
        String databaseName = resource.getDatabase();
        if (this.blacklistResources.containsKey(databaseName)) {
            List<ResourceFilter> filters = this.blacklistResources.get(databaseName);
            if (filters != null && filters.size() > 0) {
                return filters.stream().anyMatch(r -> r.getCollection().equals(resource.getCollection()));
            }
        }
        return false;
    }
}
