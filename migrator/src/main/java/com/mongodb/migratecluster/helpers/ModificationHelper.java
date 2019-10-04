package com.mongodb.migratecluster.helpers;

import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.migrators.CollectionDataMigrator;
import com.mongodb.migratecluster.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ModificationHelper {
    final static Logger logger = LoggerFactory.getLogger(CollectionDataMigrator.class);
    private final Map<String, String> renames;

    public ModificationHelper(ApplicationOptions options) {
        this.renames = options.getRenameNamespaces();
    }

    public Resource getMappedResource(Resource resource) {
        String namespace = resource.getNamespace();
        if (!renames.containsKey(namespace)) {
            return resource;
        }
        String mappedNamespace = renames.get(namespace);
        Resource mappedResource = new Resource(mappedNamespace);
        logger.debug("Found mapping for requested resource {}. Returning mapped resource {}", resource, mappedResource);
        return mappedResource;
    }

    public String getMappedNamespace(String namespace) {
        if (!renames.containsKey(namespace)) {
            return namespace;
        }
        String mappedNamespace = renames.get(namespace);
        logger.debug("Found mapping for requested resource {}. Returning mapped resource {}", namespace, mappedNamespace);
        return mappedNamespace;
    }
}
