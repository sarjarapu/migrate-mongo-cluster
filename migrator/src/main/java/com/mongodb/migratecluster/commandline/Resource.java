package com.mongodb.migratecluster.commandline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * File: Resource
 * Author: shyam.arjarapu
 * Date: 4/15/17 11:48 PM
 * Description:
 */
public class Resource {
    private String database;
    private String collection;

    public Resource() { }

    public Resource(String database, String collection) {
        this.database = database;
        this.collection = collection;
    }

    @JsonProperty("database")
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @JsonProperty("collection")
    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    @JsonIgnore
    public String getNamespace() {
        if (this.isEntireDatabase()) {
            return this.getDatabase();
        }
        return String.format("%s.%s", this.getDatabase(), this.getCollection());
    }

    @JsonIgnore
    public boolean isEntireDatabase() {
        return (this.collection.equals("{}"));
    }

    @Override
    public String toString() {
        return String.format("{ database: \"%s\", collection: \"%s\" }",
                this.getDatabase(), this.getCollection() );
    }
}
