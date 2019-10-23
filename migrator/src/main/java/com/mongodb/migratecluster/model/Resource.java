package com.mongodb.migratecluster.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

/**
 * File: Resource
 * Author: Shyam Arjarapu
 * Date: 1/11/19 9:30 PM
 * Description:
 *
 * a class representing a resource currently being processed.
 * it could be a collection inside a database
 */
public class Resource {
    private Document collectionOptions;
    private String database;
    private String collection;

    public Resource() {
        this.collectionOptions = new Document();
    }


    public Resource(String namespace) {
        this();
        String[] parts = namespace.split("\\.");
        this.database = parts[0];
        this.collection = namespace.substring(this.database.length()+1);
    }

    public Resource(String database, String collection) {
        this();
        this.database = database;
        this.collection = collection;
    }

    /**
     * Resource constructor
     *
     * @param database a string representing the database name
     * @param collection a string representing the collection name
     * @param collectionOptions a document object represending all the custom options for the collection
     */
    public Resource(String database, String collection, Document collectionOptions) {
        this(database, collection);
        this.collectionOptions = collectionOptions;
    }

    /**
     * Get's the name of the database
     *
     * @return a string representing the database
     */
    @JsonProperty("database")
    public String getDatabase() {
        return database;
    }

    /**
     * Get's the name of the collection
     *
     * @return a string representing the collection
     */
    @JsonProperty("collection")
    public String getCollection() {
        return collection;
    }

    /**
     * Get's all the custom options used at the collection
     *
     * @return a document representing custom options at the collection level
     */
    @JsonIgnore
    public Document getCollectionOptions() {
        return collectionOptions;
    }

    /**
     * Get's the namespace of the resource
     *
     * @return a string of the namespace
     */
    @JsonIgnore
    public String getNamespace() {
        if (this.isEntireDatabase()) {
            return this.getDatabase();
        }
        return String.format("%s.%s", this.getDatabase(), this.getCollection());
    }

    /**
     * Indicates of the Resource represents entire database
     *
     * @return a boolean representing if the resource represents entire database or not
     */
    @JsonIgnore
    public boolean isEntireDatabase() {
        return (this.collection.equals("{}"));
    }

    /**
     * @return a string representation of the Resource object
     */
    @Override
    public String toString() {
        return String.format("{ database: \"%s\", collection: \"%s\" }",
                this.getDatabase(), this.getCollection() );
    }
}
