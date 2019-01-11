#!/bin/sh

# kill all existing mongo processes 
killall mongos mongod mongo

# drop any previously used folders and recreate it
rm -rf data 
mkdir data 
cd data 

# create folders for source, target and oplog
mkdir source-cluster target-cluster oplog-store

# create source cluster 
cd source-cluster
mkdir -p data/replset/rs{1,2,3}/db 
sourceBin=/opt/mongodb/v3.4.18/bin
$sourceBin/mongod --replSet replset --dbpath data/replset/rs1/db --logpath data/replset/rs1/mongod.log --port 18000 --bind_ip 0.0.0.0 --logappend --fork
$sourceBin/mongod --replSet replset --dbpath data/replset/rs2/db --logpath data/replset/rs2/mongod.log --port 18001 --bind_ip 0.0.0.0 --logappend --fork
$sourceBin/mongod --replSet replset --dbpath data/replset/rs3/db --logpath data/replset/rs3/mongod.log --port 18002 --bind_ip 0.0.0.0 --logappend --fork
sleep 5
# make sure you can ping the servername otherwise add it to /etc/hosts file
SERVER_NAME=`hostname -f`
tee initiate.js  <<EOF 
rs.initiate({
    _id: "replset",
    members: [
        { _id: 0, host: "$SERVER_NAME:18000" },
        { _id: 1, host: "$SERVER_NAME:18001" },
        { _id: 2, host: "$SERVER_NAME:18002" }
    ]
});
EOF
$sourceBin/mongo admin --port 18000 < initiate.js




# create target cluster 
cd ../target-cluster
mkdir -p data/replset/rs{1,2,3}/db 
targetBin=/opt/mongodb/v3.4.18/bin
$targetBin/mongod --replSet replset --dbpath data/replset/rs1/db --logpath data/replset/rs1/mongod.log --port 18100 --bind_ip 0.0.0.0 --logappend --fork
$targetBin/mongod --replSet replset --dbpath data/replset/rs2/db --logpath data/replset/rs2/mongod.log --port 18101 --bind_ip 0.0.0.0 --logappend --fork
$targetBin/mongod --replSet replset --dbpath data/replset/rs3/db --logpath data/replset/rs3/mongod.log --port 18102 --bind_ip 0.0.0.0 --logappend --fork
sleep 5
# make sure you can ping the servername otherwise add it to /etc/hosts file
SERVER_NAME=`hostname -f`
tee initiate.js  <<EOF 
rs.initiate({
    _id: "replset",
    members: [
        { _id: 0, host: "$SERVER_NAME:18100" },
        { _id: 1, host: "$SERVER_NAME:18101" },
        { _id: 2, host: "$SERVER_NAME:18102" }
    ]
});
EOF
$targetBin/mongo admin --port 18100  < initiate.js


# mlaunch init --port 18200 --replicaset --nodes 3 --name rsOplog
# # mlaunch init --port 18100 --replicaset --nodes 3 --shards 3 --csrs
# # mlaunch init --port 18100 --replicaset --nodes 3

# create oplog replicaset 
cd ../oplog-store
mkdir -p data/rsOplog/rs{1,2,3}/db 
oplogBin=/opt/mongodb/v3.4.18/bin
$oplogBin/mongod --replSet rsOplog --dbpath data/rsOplog/rs1/db --logpath data/rsOplog/rs1/mongod.log --port 18200 --bind_ip 0.0.0.0 --logappend --fork
$oplogBin/mongod --replSet rsOplog --dbpath data/rsOplog/rs2/db --logpath data/rsOplog/rs2/mongod.log --port 18201 --bind_ip 0.0.0.0 --logappend --fork
$oplogBin/mongod --replSet rsOplog --dbpath data/rsOplog/rs3/db --logpath data/rsOplog/rs3/mongod.log --port 18202 --bind_ip 0.0.0.0 --logappend --fork
sleep 5

# make sure you can ping the servername otherwise add it to /etc/hosts file
SERVER_NAME=`hostname -f`
tee initiate.js  <<EOF 
rs.initiate({
    _id: "rsOplog",
    members: [
        { _id: 0, host: "$SERVER_NAME:18200" },
        { _id: 1, host: "$SERVER_NAME:18201" },
        { _id: 2, host: "$SERVER_NAME:18202" }
    ]
});
EOF
$oplogBin/mongo admin --port 18200  < initiate.js

cd ../