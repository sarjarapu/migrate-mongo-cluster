package com.mongodb.migratecluster.predicates;

import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import io.reactivex.functions.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * File: CollectionFilterPredicate
 * Author: shyam.arjarapu
 * Date: 4/26/17 7:11 PM
 * Description:
 */
public class CollectionFilterPredicate extends BaseResourcePredicate implements Predicate<Resource> {
    private final static Logger logger = LoggerFactory.getLogger(CollectionFilterPredicate.class);

    public CollectionFilterPredicate(List<ResourceFilter> filters) {
        super(filters);
    }

    @Override
    public boolean test(Resource resource) throws Exception {
        boolean blacklisted = isResourceInBlackList(resource);
        if (blacklisted) {
            logger.info("Skipping collection: {}; As it is marked as black listed in configuration", resource.getNamespace());
        }
        return !blacklisted;
    }

}
