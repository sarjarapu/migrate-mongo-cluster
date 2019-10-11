package com.mongodb.migratecluster.migrators;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.migratecluster.AppException;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ResourceFilter;
import com.mongodb.migratecluster.helpers.ModificationHelper;
import com.mongodb.migratecluster.helpers.MongoDBHelper;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;

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
	private ConcurrentMap<String, List<ChangeStreamDocument<Document>>> queue;
	private long id = 100000;

	public ChangestreamMigrator(ApplicationOptions options) {
		super(options);
		List<ResourceFilter> blacklistFilter = options.getBlackListFilter();
		databasePredicate = new DatabaseFilterPredicate(blacklistFilter);
		collectionPredicate = new CollectionFilterPredicate(blacklistFilter);
		allowedNamespaces = new HashMap<>();
		queue = new ConcurrentHashMap<>();
	}

	@Override
	public void process() throws AppException {
		sendData(this.getTargetClient());
		// needs to be made more robust, preferably with a resumetoken and/or some timestamp
		logger.trace("SourceClient: {}", this.getSourceClient());
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
					addToQueue(doc, currentNamespace);					
				} else {
					logger.error("process: Namespace without dot!");
					throw new RuntimeException("ChangestreamMigrator - write - namespace without dot");
				}

			} else {
				logger.trace("database/collection blacklisted {}", doc.getNamespace());
			}

		}	
		logger.info("Exiting changestream");

	}

	private void sendData(MongoClient targetClient) {
		logger.debug("sendData preparation");;
		int poolSize = 1;
		ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1));
		executor.submit(() -> { 
			logger.debug("sendData thread");
			while (true) {
				for (Entry<String, List<ChangeStreamDocument<Document>>> e: queue.entrySet()) {
					String namespace = e.getKey();
					List<ChangeStreamDocument<Document>> queueList = e.getValue();
					int size = queueList.size();
					if (size > 0 ) {
						logger.debug("Trying to send {} to {}", size, namespace);
						try {
							BulkWriteResult wr = sendData(targetClient, namespace, queueList, size);
							if (wr != null ) {
								logger.debug("writeResult {}", wr.toString());
							} else {
								logger.debug("writeResult null");
							}
						} catch (BulkWriteException bwe) {
							logger.warn("Ignoring BulkWriteException {}", bwe.getMessage());
						}
					}
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					logger.error("Interrupted sleep: {}", e1.getMessage());
				}
			}
			// logger.info("sendData end");
		});

	}

	private BulkWriteResult sendData(MongoClient targetClient, String namespace, List<ChangeStreamDocument<Document>> queueList, int size) throws AppException {
		int dot = namespace.indexOf('.');
		String database = namespace.substring(0, dot);
		String collection = namespace.substring(dot+1);
		MongoCollection<Document> coll = targetClient.getDatabase(database).getCollection(collection);

		List<WriteModel<Document>> operations = new ArrayList<>();

		for (int i = 0; i < size; i++) {
			ChangeStreamDocument<Document> d = queueList.get(i);
			WriteModel<Document> model = null;
			if (d.getOperationType() == OperationType.INSERT ) {
				model = new InsertOneModel<Document>(d.getFullDocument());
			} else if (d.getOperationType() == OperationType.UPDATE) {
				Bson filter = eq("_id", d.getFullDocument().get("_id"));
				model = new ReplaceOneModel<Document>(filter, d.getFullDocument());
			} else if (d.getOperationType() == OperationType.DELETE) {
				Bson filter = eq("_id", d.getFullDocument().get("_id"));
				model = new DeleteOneModel<Document>(filter);
			}
			if (model == null) {
				logger.error("[UNHANDLED] do not recognize operation {}", d.getOperationType());
				logger.error("[UNHANDLED] changestreamdoc: {}", d);
			} else {
				operations.add(model);
			}
		}
		logger.debug("Accumulated {} ops for {}", operations.size(), namespace);
		// remove processed list elements - is that safe here? Do we want to store operations somewhere in case something fails?
		queueList.subList(0, size).clear();
		logger.trace("Cleared queue");
		try {
			BulkWriteOptions options = new BulkWriteOptions();
			options.ordered(false);
			logger.trace("Sending bulkwrite");
			BulkWriteResult writeResult = coll.bulkWrite(operations, options);
			logger.trace("Sent bulkwrite");
			return writeResult;
		} catch (BulkWriteException bwe) {
			logger.warn("Ignoring BulkWriteException {}", bwe.getMessage());
		} catch (Exception e) {
			logger.warn("Ignoring exception {}", e.getMessage());
		}
		// not sure if this is ok
		return null;
	}

	private void addToQueue(ChangeStreamDocument<Document> doc, String namespace) {

		if (!queue.containsKey(namespace) ) {
			queue.put(namespace, Collections.synchronizedList(new ArrayList<>()));
		}
		List<ChangeStreamDocument<Document>> list = queue.get(namespace);
		list.add(doc);
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
		logger.info("ChangestreamMigrator preprocess - getting operationTime");
		MongoClient client = this.getSourceClient();
		Document ping = new Document("ping", "1");
		Document reply = client.getDatabase("admin").runCommand(ping);
		operationTime = reply.get("operationTime", BsonTimestamp.class);
		if (operationTime == null) {
			logger.error("operationTime MUST NOT be null");
		}
	}

}
