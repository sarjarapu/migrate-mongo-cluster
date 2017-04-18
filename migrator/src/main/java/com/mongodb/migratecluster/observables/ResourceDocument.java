package com.mongodb.migratecluster.observables;

import com.mongodb.migratecluster.commandline.Resource;
import org.bson.Document;

/**
 * Created by shyamarjarapu on 4/17/17.
 */
public class ResourceDocument {
    private final Resource resource;
    private final Document document;

    public ResourceDocument(Resource resource, Document document) {
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
