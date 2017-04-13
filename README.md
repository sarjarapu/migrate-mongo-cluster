# Build and run 

git clone git@github.com:sarjarapu/migrate-mongo-cluster.git

cd migrate-mongo-cluster/migrator
mvn clean compile package
java -jar target/migrate-mongo-cluster-1.0-SNAPSHOT-jar-with-dependencies.jar

