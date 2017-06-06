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
mlaunch --port 18000 --replicaset --nodes 3


# create target cluster 
cd ../target-cluster
# mlaunch init --port 18100 --replicaset --nodes 3 --shards 3 --csrs
mlaunch init --port 18100 --replicaset --nodes 3

# create oplog replicaset 
cd ../oplog-store
mlaunch init --port 18200 --replicaset --nodes 3 --name rsOplog

