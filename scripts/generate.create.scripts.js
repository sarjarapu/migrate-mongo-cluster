/*
Author: Shyam Arjarapu
Description: Generate create collection and index scripts for 
every collection in every database in the source cluster. Run 
this code in the MongoDB shell, which spits the output scripts.
These scripts can be used to recreate the schema, indexes etc
on the target environment of your choice.
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