package com.mongodb.migratecluster.commandline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by shyamarjarapu on 4/14/17.
 */
public class ResourceFilter {
    private String database;
    private String collection;
    private String filterExpression;


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

    @JsonProperty("filterExpression")
    public String getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(String filterExpression) {
        this.filterExpression = filterExpression;
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
        return String.format("{ database: \"%s\", collection: \"%s\", filterExpression: %s }",
                this.getDatabase(), this.getCollection(), this.getFilterExpression());
    }
}
