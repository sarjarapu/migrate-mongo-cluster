package com.mongodb.migratecluster.predicates;

import com.mongodb.migratecluster.commandline.ResourceFilter;
import io.reactivex.functions.Predicate;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * File: DatabaseFilterPredicate
 * Author: shyam.arjarapu
 * Date: 4/26/17 7:11 PM
 * Description:
 */
public class DatabaseFilterPredicate extends BaseResourcePredicate implements Predicate<Document> {
    private final static Logger logger = LoggerFactory.getLogger(DatabaseFilterPredicate.class);

    public DatabaseFilterPredicate(List<ResourceFilter> filters) {
        super(filters);
    }

    @Override
    public boolean test(Document dbDocument) throws Exception {
        String databaseName = dbDocument.getString("name");
        boolean blacklisted = isDatabaseInBlackList(databaseName);
        if (blacklisted) {
            logger.info("Skipping database: {}; As it is marked as black listed in configuration", databaseName);
        }
        return !blacklisted;
    }

}
