package pl.edu.mimuw.mapreduce.worker.util;

import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;

public class Combiner {

    private final Storage storage;
    private final long dataDir;
    private final long destinationId;
    private final File binary;

    public Combiner(Storage storage, long dataDir, long destinationId, File binary) {
        this.storage = storage;
        this.dataDir = dataDir;
        this.destinationId = destinationId;
        this.binary = binary;
    }

    /**
     * Combines results from two adjacent splits s1, s2, stores the result in file with id split1.beg, which represents
     * a result for a split [s1.beg, s2.end] = merge(s1,s2)
     */
    public void combine(Split s1, Split s2) {
        if (s2.getEnd() == s1.getBeg()) {
            combine(s2, s1);
            return;
        }
        if (s1.getEnd() != s2.getBeg()) throw new RuntimeException("splits are not adjacent");
        combineFiles(s1.getEnd(), s2.getBeg());
    }

    public void combine(SplitBuilder s1, SplitBuilder s2) {
        if (s2.getEnd() == s1.getBeg()) {
            combine(s2, s1);
            return;
        }
        if (s1.getEnd() != s2.getBeg()) throw new RuntimeException("splits are not adjacent");
        combineFiles(s1.getEnd(), s2.getBeg());
    }

    private void combineFiles(long fileId1, long fileId2) {
        throw new RuntimeException("todo");
    }
}
