package com.mongodb.migratecluster.observables;

import com.mongodb.migratecluster.commandline.Resource;
import org.bson.Document;

/**
 * File: ResourceDocument
 * Author: shyam.arjarapu
 * Date: 4/17/17 11:36 PM
 * Description:
 */
public class ResourceDocument {
    private final Resource resource;
    private final Document document;

    ResourceDocument(Resource resource, Document document) {
        this.resource = resource;
        this.document = document;
    }

    public Resource getResource() {
        return resource;
    }

    public Document getDocument() {
        return document;
    }
}
