package com.mongodb.migratecluster.model;

import org.bson.Document;

import java.util.List;

/**
 *
 * File: DocumentsBatch
 * Author: Shyam Arjarapu
 * Date: 1/11/19 8:25 PM
 * Description:
 * a class representing a batch of documents processed for a given resource.
 */
public class DocumentsBatch {
    private final int batchId;
    private final Resource resource;
    private final List<Document> documents;

    /**
     * @param resource a resource representing database and collection
     * @param batchId a identifier for the batch of documents
     * @param documents a list of documents in the current batch
     *
     * @see Resource
     */
    public DocumentsBatch(Resource resource, int batchId, List<Document> documents) {
        this.resource = resource;
        this.batchId = batchId;
        this.documents = documents;
    }

    /**
     * Get's the current batch identifier
     *
     * @return an identifier representing the current batch
     */
    public int getBatchId() {
        return batchId;
    }

    /**
     * Get's the resource of current batch
     *
     * @return the resource which the batch represents
     * @see Resource
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * @return a list of all the Documents in this Batch
     * @see Document
     */
    public List<Document> getDocuments() {
        return documents;
    }

    /**
     * @return the size of the documents in the current batch
     */
    public int getSize() {
        return documents.size();
    }

    /**
     * @return a string representation of the Documents Batch object
     */
    @Override
    public String toString() {
        return String.format("{ resource: %s; batchId: %d, size: %d }",
                resource.toString(), batchId, documents.size());
    }
}
