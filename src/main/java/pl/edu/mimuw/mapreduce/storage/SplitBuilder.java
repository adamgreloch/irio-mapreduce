package pl.edu.mimuw.mapreduce.storage;

import pl.edu.mimuw.proto.common.Split;

public class SplitBuilder {
    private long beg;
    private long end;

    public SplitBuilder(long beg, long end) {
        this.beg = beg;
        this.end = end;
    }

    public static SplitBuilder merge(SplitBuilder s1, SplitBuilder s2) {
        if (s2.end == s1.beg) return merge(s2, s1);
        if (s1.end != s2.beg) throw new RuntimeException("splits are not adjacent");
        s1.end = s2.end;
        return s1;
    }

    public Split build() {
        return Split.newBuilder().setBeg(beg).setEnd(end).build();
    }

    public long getBeg() {
        return beg;
    }

    public long getEnd() {
        return end;
    }
}
