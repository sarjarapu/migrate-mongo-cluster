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
    private Long saveFrequency;
    private boolean oplogOnly;
    private String saveOplogTag;
    private Long batchSize;
    private List<ResourceFilter> whiteListFilter;

    public ApplicationOptions() {
        sourceCluster = "";
        targetCluster = "";
        oplogStore = "";
        configFilePath = "";
        showHelp = false;
        dropTarget = false;
        oplogOnly = false;
        saveFrequency = 1L;
        saveOplogTag = "";
        batchSize = 1000L;
        setBlackListFilter(new ArrayList<>());
        setWhiteListFilter(new ArrayList<>());
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

    @JsonProperty("oplogOnly")
    public boolean isOplogOnly() {
        return oplogOnly;
    }

    public void setOplogOnly(boolean oplogOnly) {
        this.oplogOnly = oplogOnly;
    }
    
    @JsonProperty("blackListFilter")
    public List<ResourceFilter> getBlackListFilter() {
        return blackListFilter;
    }

    public void setBlackListFilter(List<ResourceFilter> blackListFilter) {
        this.blackListFilter = blackListFilter;
    }

    @JsonProperty("whiteListFilter")
    public List<ResourceFilter> getWhiteListFilter() {
        return whiteListFilter;
    }

    public void setWhiteListFilter(List<ResourceFilter> whiteListFilter) {
        this.whiteListFilter = whiteListFilter;
    }
    
    @Override
    public String toString() {
        return String.format("{ showHelp : %s, configFilePath: \"%s\", " +
                " sourceCluster: \"%s\", targetCluster: \"%s\", batchSize: %s," +
                " oplog: \"%s\", drop: %s, oplogOnly: %s, saveFrequency: %s, saveOplogTag: %s" +
                "blackListFilter: %s, whiteListFilter: %s }",
                this.isShowHelp(), this.getConfigFilePath(), this.getSourceCluster(),
                this.getTargetCluster(), this.getBatchSize(),this.getOplogStore(), this.isDropTarget(),
                this.isOplogOnly(),this.getSaveFrequency(), this.getSaveOplogTag(),
                ListUtils.select(this.getBlackListFilter(), f -> f.toString()),
                ListUtils.select(this.getWhiteListFilter(), f -> f.toString()));
    }

    @JsonProperty("renameNamespaces")
	public Map<String, String> getRenameNamespaces() {
		return renames;
	}
    
    public void setRenames(Map<String, String> renames) {
    	this.renames = renames;
    }
    
    @JsonProperty("saveFrequency")
    public void setSaveFrequency(Long saveFrequency) {
    	this.saveFrequency = saveFrequency;
    }
    public Long getSaveFrequency() {
    	return saveFrequency;
    }
    
    @JsonProperty("saveOplogTag")
    public void setSaveOplogTag(String saveOplogTag) {
    	this.saveOplogTag = saveOplogTag;
    }
    public String getSaveOplogTag() {
    	return saveOplogTag;
    }
    
    @JsonProperty("batchSize")
    public void setBatchSize(Long batchSize) {
    	this.batchSize = batchSize;
    }
    public Long getBatchSize() {
    	return batchSize ;
    }
}
