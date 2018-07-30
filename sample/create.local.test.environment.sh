#!/bin/sh

# kill all existing mongo processes 
killall mongos mongod mongo


# drop any previously used folders 
rm -rf data 
# create data folder
mkdir data 
cd data 

# create folders for source, target and oplog
mkdir source-cluster target-cluster oplog-store

# create source cluster 
cd source-cluster
# mlaunch init --port 18000 --replicaset --nodes 3 --shards 3 --csrs
# mlaunch --port 18000 --replicaset --nodes 3
mkdir -p data/replset/rs{1,2,3}/db 
sourceBin=/opt/mongodb/v3.0.8/bin
$sourceBin/mongod --replSet replset --dbpath data/replset/rs1/db --logpath data/replset/rs1/mongod.log --port 18000 --logappend --fork --storageEngine mmapv1
$sourceBin/mongod --replSet replset --dbpath data/replset/rs2/db --logpath data/replset/rs2/mongod.log --port 18001 --logappend --fork --storageEngine mmapv1
$sourceBin/mongod --replSet replset --dbpath data/replset/rs3/db --logpath data/replset/rs3/mongod.log --port 18002 --logappend --fork --storageEngine mmapv1
sleep 5
# make sure you fix the machine name to yours 
$sourceBin/mongo admin --port 18000 <<EOF 
rs.initiate();
rs.add('127.0.0.1:18001')
rs.add('127.0.0.1:18002')
EOF




# create target cluster 
cd ../target-cluster
# mlaunch init --port 18100 --replicaset --nodes 3 --shards 3 --csrs
mlaunch init --port 18100 --replicaset --nodes 3
# mkdir -p data/replset/rs{1,2,3}/db 
# targetBin=/opt/mongodb/v3.4.10/bin
# $targetBin/mongod --replSet replset --dbpath data/replset/rs1/db --logpath data/replset/rs1/mongod.log --port 18100 --logappend --fork
# $targetBin/mongod --replSet replset --dbpath data/replset/rs2/db --logpath data/replset/rs2/mongod.log --port 18101 --logappend --fork
# $targetBin/mongod --replSet replset --dbpath data/replset/rs3/db --logpath data/replset/rs3/mongod.log --port 18102 --logappend --fork
# sleep 5
# # make sure you fix the machine name to yours 
# $targetBin/mongo admin --port 18100 <<EOF 
# rs.initiate();
# rs.add('ip-172-31-29-177:18101')
# rs.add('ip-172-31-29-177:18102')
# EOF



# create oplog replicaset 
cd ../oplog-store
mlaunch init --port 18200 --replicaset --nodes 3 --name rsOplog
# # mlaunch init --port 18100 --replicaset --nodes 3 --shards 3 --csrs
# # mlaunch init --port 18100 --replicaset --nodes 3
# mkdir -p data/replset/rs{1,2,3}/db 
# targetBin=/opt/mongodb/v3.4.10/bin
# $targetBin/mongod --replSet rsOplog --dbpath data/rsOplog/rs1/db --logpath data/replset/rs1/mongod.log --port 18100 --logappend --fork
# $targetBin/mongod --replSet rsOplog --dbpath data/rsOplog/rs2/db --logpath data/replset/rs2/mongod.log --port 18101 --logappend --fork
# $targetBin/mongod --replSet rsOplog --dbpath data/rsOplog/rs3/db --logpath data/replset/rs3/mongod.log --port 18102 --logappend --fork
# sleep 5
# # make sure you fix the machine name to yours 
# $targetBin/mongo admin --port 18100 <<EOF 
# rs.initiate();
# rs.add('ip-172-31-29-177:18101')
# rs.add('ip-172-31-29-177:18102')
# EOF


cd ../