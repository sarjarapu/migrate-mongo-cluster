package com.mongodb.migratecluster.commandline;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * File: ResourceFilter
 * Author: shyam.arjarapu
 * Date: 4/14/17 11:48 PM
 * Description:
 */
public class ResourceFilter extends Resource {
    private String filterExpression;


    @JsonProperty("filterExpression")
    public String getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(String filterExpression) {
        this.filterExpression = filterExpression;
    }

    @Override
    public String toString() {
        String value = super.toString();
        return String.format("{ %s, filterExpression: %s }",
                value.substring(1, value.length()-2),
                this.getFilterExpression());
    }
}
