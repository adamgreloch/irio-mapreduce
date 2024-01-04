package pl.edu.mimuw.mapreduce.storage.local;

/**
 * Split is a slice of LocalStorage's file list
 */

public class Split {
    private final long splitBegin;
    private final long splitEnd;
    private final long splitSize;

    public Split(long splitBegin, long splitEnd, long splitSize) {
        this.splitBegin = splitBegin;
        this.splitEnd = splitEnd;
        this.splitSize = splitSize;
    }

    public long getSplitBegin() {
        return splitBegin;
    }

    public long getSplitEnd() {
        return splitEnd;
    }

    public long getSplitSize() {
        return splitSize;
    }
}
