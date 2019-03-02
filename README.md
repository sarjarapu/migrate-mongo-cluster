# Overview

## About the application

The migrate-mongo-cluster is an application to help you migrate the data from one server to the other. The objective of this application is to help you acheive live migration of the data from source database to target database. This application comes handy especially, when you are in shared cluster and want to change the shard key without unsharding and resharding. 

## What / How it does

From technical stand point of view, the application reads data document by document from the source database and writes them into the target database. The application also tails the oplog and reapply them on target once it copied all data. 

## Word of caution

Ideally, one should be using a backup of existing database, restore it to the server were you wanted to migrate, let the oplog catchup and re-elect the new server as primary. If for whatever reason, you cannot acheive the above recommended approach, you may use this application to do the migration, **at your own risk!**

## Assumptions

### Disable the balancer during the migration process

If you are on shareded cluser, please make sure to [disable the balancer](https://docs.mongodb.com/manual/tutorial/manage-sharded-cluster-balancer/#sharding-balancing-disable-temporarily) for the entire duration of the migation. 

### OplogSize is big enough to hold entries for the entire duration

This application records the recent oplog entry on the replica set at the time of first run (or everytime if `drop` option is set). Once the application copies all the data from source onto the target, it tails the source oplog from the recorded oplog timestamp. For the migration process to successfully complete, it's crucial to have oplog big enough to accomidate the operations for the anticipated duration of the process. Please refer to the MongoDB documentation, [Change the Size of the Oplog](https://docs.mongodb.com/manual/tutorial/change-oplog-size/index.html) to an appropriate value.


### Collections, Indexes and Users needs to be precreated

While migrating the data from source to target, it is assumed that all the indexes of your interest are precreated on target before beginning the migration. 

If you are planning to change the shard key then the application assumes that you configured the sharded collections accordingly.

### Script to precreate collections and indexes

You may use the below script to help generate code for creating the collections and the indexes for target. 

- Open the MongoDB shell connected to the source
- Copy paste the script (below) in the MongoDB shell
- The script generates the commands that you need to run on Targer
- Copy the generated scripts
- Open the MongoDB shell connected to the target
- Paste / run the previously generated scripts

```js
/*
Author: Shyam Arjarapu
Description: Generate create collection and index scripts for every collection in every database
*/

var collections = [];
db.getMongo().getDBs().databases.forEach(function(databaseMeta){
    if (databaseMeta.name == 'admin' || databaseMeta.name == 'local' || databaseMeta.name == 'config')
    return;
	var database = db.getSiblingDB(databaseMeta.name);
	database.getCollectionInfos().forEach(function(collectionInfo){
        var collection = {
            name: collectionInfo.name,
            database: databaseMeta.name,
            options: collectionInfo.options
        };
		collections.push(collection);
        var ignoreKeys = [ "v", "key", "name", "ns" ];
        var indexes = database.getCollection(collection.name)
            .getIndexes()
            .map( function(item) {
                var index = {
                    key: item.key,
                    name: item.name
                };
                var options = {};
                Object.keys(item).filter(key => !Array.contains(ignoreKeys, key)).forEach(key => options[key] = item[key]);
                if (Object.keys(options).length > 0) {
                    index.options = options;
                }
                return index;
            });
        collection.indexes = indexes;
	});
});
var collectionStrings = collections.map(function(collection){
    return `db.getSiblingDB('${collection.database}').createCollection('${collection.name}', ${JSON.stringify(collection.options)})`;
}).join("\n");
var indexStrings = collections.map(function(collection){
    return collection.indexes.map(function(index){
        var optionsJSON = index.options ? `, ${JSON.stringify(index.options)}` : '';
        return `db.getSiblingDB('${collection.database}').getCollection('${collection.name}').createIndex(${JSON.stringify(index.key)} ${optionsJSON});`
    }).join("\n");
}).join("\n");
print(collectionStrings + "\n" + indexStrings);

```

## How to run the application

### Download from git

```bash
git clone git@github.com:sarjarapu/migrate-mongo-cluster.git
```

## Build using Maven

```bash
cd migrate-mongo-cluster/migrator
mvn clean compile package
```

## Help instructions

```bash
java -jar target/migrate-mongo-cluster-1.0-SNAPSHOT-jar-with-dependencies.jar -h

usage: migratecluster [-c <arg>] [-d] [-h] [-o <arg>] [-s <arg>] [-t <arg>]  
 -c,--config <arg>   configuration file for migration  
 -d,--drop           drop target collections before copying  
 -h,--help           print this message  
 -o,--oplog <arg>    oplog store connection string  
 -s,--source <arg>   source cluster connection string  
 -t,--target <arg>   target cluster connection string  
 -m,--mode <arg>     migration mode. Supported modes: oplogOnly
```

## Run the application using sample migration

```bash
java -jar target/migrate-mongo-cluster-1.0-SNAPSHOT-jar-with-dependencies.jar -c ../sample/sample-migration.conf
```

# Features to be build into program

Below are the list of features that I thought of incorporating into the application.

- [x] Get databases, collections and docs
- [x] Save the documents onto target server
- [x] Reactive Programming
- [x] Buffered read / Bulk write 
- [x] Multithreading - Read full documents in a different thread
- [x] Multithreading - Write full documents in a different thread
- [x] Drop database / collection before inserting
- [x] Oplog tail for each replicaSet 
- [x] Continuation from where we left off
- [x] Error handling, duplicate key, etc
- [x] Use connection string with all the members in replicaset
- [x] Retry logic when the primary is down
- [x] Read preference - secondary from source?
- [ ] Status Database to keep track of progress
- [ ] API to expose status of migrators from database
- [ ] Runtime injection of the log level
- [x] While copying find the id and continue where you left off
- [ ] Move the gapWatcher out of the oplogMigrator
- [x] Apply the oplogs in bulk operations
- [ ] Use the readPreference on collection vs on client
- [ ] How do you track the multi shard -> mongos and last known id (lastknownid should be / rs)
- [ ] I don't think you are saving the recentDocumentId properly
- [ ] Target is behind by 446 seconds & 0000 operations; even after completing the transfer
- [ ] Add [backpressure](https://vlkan.com/blog/post/2016/07/20/rxjava-backpressure/) feature
- [ ] [SyncOnSubscribe](https://www.littlerobots.nl/blog/Note-to-self-RxJava-SyncOnSubscribe/)
- [ ] https://praveer09.github.io/technology/2016/02/29/rxjava-part-3-multithreading/
- [ ] https://blog.gojekengineering.com/multi-threading-like-a-boss-in-android-with-rxjava-2-b8b7cf6eb5e2

## Random notes

-  I think oplog tail should make entry if not already exists and start saving tail to oplog tail db this will help in scenarios when source oplog headroom is small compared to time it takes to populate all the historical data. for simplicity i assume oplog is big enough for multiple days if an oplog entry already exists then wait till all the copy process is done begin the oplog tail apply operations only after copy process is completed.