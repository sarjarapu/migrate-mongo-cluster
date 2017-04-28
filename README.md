## Download from git
git clone git@github.com:sarjarapu/migrate-mongo-cluster.git

## Build using Maven
cd migrate-mongo-cluster/migrator
mvn clean compile package

## Assumptions
### Target shard keys are precreated
While migrating the data from source to target, it is assumed that new shard key of your interest is precreated at target before beginning the migration

## Help instructions
java -jar target/migrate-mongo-cluster-1.0-SNAPSHOT-jar-with-dependencies.jar -h

usage: migratecluster [-c <arg>] [-d] [-h] [-o <arg>] [-s <arg>] [-t <arg>]  
 -c,--config <arg>   configuration file for migration  
 -d,--drop           drop target collections before copying  
 -h,--help           print this message  
 -o,--oplog <arg>    oplog store connection string  
 -s,--source <arg>   source cluster connection string  
 -t,--target <arg>   target cluster connection string  


## Run the application using sample migration
java -jar target/migrate-mongo-cluster-1.0-SNAPSHOT-jar-with-dependencies.jar -c ../sample/sample-migration.conf 


## Features to be build into program

Below are the list of features that I thought of incorporating into the application.

- [x] Get databases, collections and docs
- [x] Save the documents onto target server 
- [x] Reactive Programming
- [x] Buffered read / Bulk write 
- [x] Multithreading
- [ ] Oplog tail for each replicaSet 
- [ ] Continuation from where we left off
- [ ] Retry logic with delay
- [ ] Error handling, duplicate key, etc
- [ ] Status Database to keep track of progress 
- [ ] API to expose status of migrators from database 


