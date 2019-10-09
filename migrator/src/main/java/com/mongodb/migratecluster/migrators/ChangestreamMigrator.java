package com.mongodb.migratecluster.migrators;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.helpers.ModificationHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;

import static com.mongodb.client.model.Filters.eq;

import java.util.HashMap;
import java.util.List;

/**
 * Mashup of OplogMigrator and OplogWriter for change streams
 *
 */
public class ChangestreamMigrator extends BaseMigrator {
	final static Logger logger = LoggerFactory.getLogger(ChangestreamMigrator.class);
	private DatabaseFilterPredicate databasePredicate;
	private CollectionFilterPredicate collectionPredicate;
	private final HashMap<String, Boolean> allowedNamespaces;
	private BsonTimestamp operationTime;

	public ChangestreamMigrator(ApplicationOptions options) {
		super(options);
		List<ResourceFilter> blacklistFilter = options.getBlackListFilter();
		databasePredicate = new DatabaseFilterPredicate(blacklistFilter);
		collectionPredicate = new CollectionFilterPredicate(blacklistFilter);
		allowedNamespaces = new HashMap<>();
		MongoClient client = this.getSourceClient();
		ClientSession session = client.startSession();
		Document ping = new Document("ping", "1");
		client.getDatabase("admin").runCommand(session, ping);
		operationTime = session.getOperationTime(); 
		session.close();
		if (operationTime == null) {
			logger.error("operationTime MUST NOT be null");
		}
	}

	@Override
	public void process() throws AppException {
		// needs to be made more robust, preferably with a resumetoken and/or some timestamp
		logger.info("SourceClient: {}", this.getSourceClient());
		ChangeStreamIterable<Document> csi = this.getSourceClient().watch().fullDocument(FullDocument.UPDATE_LOOKUP).startAtOperationTime(operationTime);
		ModificationHelper modificationHelper = new ModificationHelper(options);

		MongoCursor<ChangeStreamDocument<Document>> iterator = csi.iterator();
		logger.info("Starting changestream");
		while (iterator.hasNext()) {
			ChangeStreamDocument<Document> doc = iterator.next();
			if (isNamespaceAllowed(doc.getNamespace().getFullName())) {
				MongoNamespace ns = doc.getNamespace();
				String namespace = ns.getDatabaseName() + "." + ns.getCollectionName();
				String currentNamespace = modificationHelper.getMappedNamespace(namespace);
				int dot = currentNamespace.indexOf('.');
				if (dot > -1) {
					// TODO refactor to use bulk operations
					String database = currentNamespace.substring(0, dot);
					String collection = currentNamespace.substring(dot+1);
					logger.info("Transferring to {} and {}", database, collection);
					Document fullDoc = doc.getFullDocument();
					MongoCollection<Document> coll = this.getTargetClient().getDatabase(database).getCollection(collection);
					try {
						if (doc.getOperationType() == OperationType.INSERT) {
							coll.insertOne(fullDoc);
						} else
							if (doc.getOperationType() == OperationType.UPDATE) {
								Bson filter = eq("_id", fullDoc.get("_id"));
								coll.replaceOne(filter, fullDoc);
							} else
								if (doc.getOperationType() == OperationType.DELETE) {
									Bson filter = eq("_id", fullDoc.get("_id"));
									coll.deleteOne(filter);
								} else {
									logger.warn("Unsupported OperationType: {}", doc.getOperationType());
								}
					} catch (Exception e) {
						logger.error("Bravely ignoring exception {}", e.getMessage());
					}
				} else {
					logger.error("process: Namespace without dot!");
				}

			} else {
				logger.trace("database/collection blacklisted {}", doc.getNamespace());
			}

		}	
		logger.info("Exiting changestream");

	}

	// Plain copy from OplogWriter
	private boolean isNamespaceAllowed(String namespace) {
		if (!allowedNamespaces.containsKey(namespace))
		{
			boolean allow = checkIfNamespaceIsAllowed(namespace);
			allowedNamespaces.put(namespace, allow);
		}
		// return cached value
		return allowedNamespaces.get(namespace);
	}

	private boolean checkIfNamespaceIsAllowed(String namespace) {
		String databaseName = namespace.split("\\.")[0];
		try {
			Document dbDocument = new Document("name", databaseName);
			boolean isNotBlacklistedDB = databasePredicate.test(dbDocument);
			if (isNotBlacklistedDB) {
				// check for collection as well
				String collectionName = namespace.substring(databaseName.length()+1);
				Resource resource = new Resource(databaseName, collectionName);
				return collectionPredicate.test(resource);
			}
			else {
				return false;
			}
		} catch (Exception e) {
			logger.error("error while testing the namespace is in black list or not");
			return false;
		}
	}


	@Override
	public void preprocess() {
		// TS: not sure what to do here
	}

}
