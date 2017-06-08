package com.mongodb.migratecluster.oplog;

import org.bson.BsonTimestamp;

/**
 * File: OplogGap
 * Author: shyam.arjarapu
 * Date: 6/7/17 2:03 PM
 * Description:
 */
public class OplogGap {
    private BsonTimestamp sourceOpTime;
    private BsonTimestamp targetOpTime;

    public OplogGap(BsonTimestamp sourceTimestamp, BsonTimestamp targetTimestamp) {
        this.sourceOpTime = sourceTimestamp;
        this.targetOpTime = (targetTimestamp == null)
                ? new BsonTimestamp(0,0)
                : targetTimestamp;
    }

    public BsonTimestamp getSourceOpTime() {
        return sourceOpTime;
    }

    public BsonTimestamp getTargetOpTime() {
        return targetOpTime;
    }

    public long getGapInSeconds() {
        return sourceOpTime.getTime() - targetOpTime.getTime();
    }

    @Override
    public String toString() {
        long gapInSeconds = this.getGapInSeconds();
        int gapByOperations = (gapInSeconds > 0)
                ? 0
                : (sourceOpTime.getInc() - targetOpTime.getInc());
        return String.format("Target is behind by %d seconds & %04d operations; Target: %s, Source: %s",
                gapInSeconds, gapByOperations,
                this.getTargetOpTime(),
                this.getSourceOpTime());
    }
}
