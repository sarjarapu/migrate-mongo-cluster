package com.mongodb.migratecluster.commandline;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.migratecluster.utils.ListUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * File: ApplicationOptions
 * Author: shyam.arjarapu
 * Date: 4/13/17 11:47 PM
 * Description:
 */
public class ApplicationOptions {
    private String sourceCluster;
    private String targetCluster;
    private String oplogStore;
    private String configFilePath;
    private boolean showHelp;
    private boolean dropTarget;
    private List<ResourceFilter> blackListFilter;
    private Map<String, String> renames;

    public ApplicationOptions() {
        sourceCluster = "";
        targetCluster = "";
        oplogStore = "";
        configFilePath = "";
        showHelp = false;
        dropTarget = false;
        setBlackListFilter(new ArrayList<>());
        setRenames(new HashMap<>());
    }


    @JsonProperty("sourceCluster")
    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    @JsonProperty("targetCluster")
    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    @JsonProperty("oplogStore")
    public String getOplogStore() {
        return oplogStore;
    }

    public void setOplogStore(String oplogStore) {
        this.oplogStore = oplogStore;
    }

    @JsonProperty("configFilePath")
    public String getConfigFilePath() {
        return configFilePath;
    }

    public void setConfigFilePath(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public boolean isShowHelp() {
        return showHelp;
    }

    public void setShowHelp(boolean showHelp) {
        this.showHelp = showHelp;
    }

    @JsonProperty("dropTarget")
    public boolean isDropTarget() {
        return dropTarget;
    }

    public void setDropTarget(boolean dropTarget) {
        this.dropTarget = dropTarget;
    }

    @JsonProperty("blackListFilter")
    public List<ResourceFilter> getBlackListFilter() {
        return blackListFilter;
    }

    public void setBlackListFilter(List<ResourceFilter> blackListFilter) {
        this.blackListFilter = blackListFilter;
    }

    @Override
    public String toString() {
        return String.format("{ showHelp : %s, configFilePath: \"%s\", " +
                " sourceCluster: \"%s\", targetCluster: \"%s\", " +
                ", oplog: \"%s\", drop: %s, blackListFilter: %s }",
                this.isShowHelp(), this.getConfigFilePath(), this.getSourceCluster(),
                this.getTargetCluster(), this.getOplogStore(), this.isDropTarget(),
                ListUtils.select(this.getBlackListFilter(), f -> f.toString()));
    }

    @JsonProperty("renames")
	public Map<String, String> getRenames() {
		return renames;
	}
    
    public void setRenames(Map<String, String> renames) {
    	this.renames = renames;
    }

}
