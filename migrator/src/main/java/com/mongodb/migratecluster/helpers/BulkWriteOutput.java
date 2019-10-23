package com.mongodb.migratecluster.helpers;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class BulkWriteOutput {
    private final BulkWriteResult result;
    private final int duplicateKeyExceptionCount;
    private final int deletedCount;
    private final int modifiedCount;
    private final int insertedCount;
    private final int upsertedCount;
    private final List<WriteModel<Document>> failedOps;

    public BulkWriteOutput(BulkWriteResult bulkWriteResult) {
        this.result = bulkWriteResult;
        this.failedOps = new ArrayList<>();
        this.deletedCount = result.getDeletedCount();
        this.modifiedCount = result.getModifiedCount();
        this.insertedCount = result.getInsertedCount();
        this.upsertedCount = result.getUpserts().size();
        this.duplicateKeyExceptionCount = 0;
    }

    public BulkWriteOutput(int deletedCount, int modifiedCount, int insertedCount, int upsertedCount, int duplicateKeyExceptionCount, List<WriteModel<Document>> failedOps) {
        this.failedOps = failedOps;
        this.result = null;
        this.deletedCount = deletedCount;
        this.modifiedCount = modifiedCount;
        this.insertedCount = insertedCount;
        this.upsertedCount = upsertedCount;
        this.duplicateKeyExceptionCount = duplicateKeyExceptionCount;
    }

    public int getSuccessfulWritesCount() {
        return deletedCount + modifiedCount + insertedCount + upsertedCount + duplicateKeyExceptionCount;
    }

    @Override
    public String toString() {
        return String.format("deletedCount: %d, modifiedCount: %d, insertedCount: %d, upsertedCount: %d, duplicateKeyExceptionCount: %d, failedOps: %s",
                deletedCount, modifiedCount, insertedCount, upsertedCount, duplicateKeyExceptionCount, failedOps.toString());
    }
}
