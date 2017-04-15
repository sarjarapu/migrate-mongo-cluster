package com.mongodb.migratecluster.migrators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by shyamarjarapu on 4/13/17.
 */
public class DataMigrator {
    final static Logger logger = LoggerFactory.getLogger(DataMigrator.class);
    private ApplicationOptions appOptions;

    public DataMigrator(ApplicationOptions appOptions) {
        this.appOptions = appOptions;
    }

    private boolean isValidOptions() {
        // on appOptions source, target, oplog must all be present
        if (
                (this.appOptions.getSourceCluster() == "") ||
                (this.appOptions.getTargetCluster() == "") ||
                (this.appOptions.getOplogStore() == "")
            ) {
            // invalid input
            return false;
        }
        return true;
    }

    public void process() throws AppException {
        // check if the appOptions are valid
        if (!this.isValidOptions()) {
            String message = String.format("invalid input args for sourceCluster, targetCluster and oplog. \ngiven: %s", this.appOptions.toString());
            throw new AppException(message);
        }

        // loop through source and copy to target
        readSourceClusterConfigDatabase();
    }

    private void readSourceClusterConfigDatabase() {
        // Use a Connection String
        String connectionString = String.format("mongodb://%s", this.appOptions.getSourceCluster());
        MongoClientURI uri = new MongoClientURI(connectionString);
        MongoClient client = new MongoClient(uri);


        ServerMigrator serverMigrator = new ServerMigrator(client);

        try {
            serverMigrator.migrate(this.appOptions);
        } catch (AppException e) {
            e.printStackTrace();
        }

        // NOTE: I would rather have pub sub of what's being read and who processes it
        // serverMigrator.migrate(targetServer);

        client.close();
    }
}
