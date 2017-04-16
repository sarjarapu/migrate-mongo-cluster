package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.utils.ListUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.mongodb.migratecluster.utils.ListUtils.*;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class ServerMigrator {
    final static Logger logger = LoggerFactory.getLogger(ServerMigrator.class);
    private List<Document> databases;
    private final MongoClient client;

    public ServerMigrator(MongoClient client) {
        this.client = client;
    }

    public void migrate(ApplicationOptions appOptions) throws AppException {
        this.databases = IteratorHelper.getDatabases(client);

        if (this.databases == null) {
            String message = "looks like migrate process was invoked before loading the databases";
            logger.warn(message);
            throw new AppException(message);
        }
        else if (this.databases.size() == 0) {
            String message = "looks like no databases found in the source";
            logger.warn(message);
            throw new AppException(message);
        }

        List<Document> filteredDatabases = getDatabaseForProcessing(appOptions);
        List<DatabaseMigrator> databaseMigrators = select(filteredDatabases,
                d -> new DatabaseMigrator(client, d));
        databaseMigrators.forEach(d -> {
            try {
                d.migrate(appOptions);
            } catch (AppException e) {
                e.printStackTrace();
            }
        });
    }

    private List<Document> getDatabaseForProcessing(ApplicationOptions appOptions) {
        // loop through databases and check if there filters
        List<ResourceFilter> filters = appOptions.getBlackListFilter();
        List<String> skipDatabases =
                select(
                    where(filters, i -> i.isEntireDatabase()),
                    d -> d.getDatabase());


        return null;
        // load all databases except for full databases that needs skip
//        return where(this.databases, d -> {
//            String db = d.getString("name");
//            return !any(skipDatabases, s -> s.getDatabase().equals(db));
//        });
    }

}
