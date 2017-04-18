## Download from git
git clone git@github.com:sarjarapu/migrate-mongo-cluster.git

## Build using Maven
cd migrate-mongo-cluster/migrator
mvn clean compile package

## Show help instructions
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

