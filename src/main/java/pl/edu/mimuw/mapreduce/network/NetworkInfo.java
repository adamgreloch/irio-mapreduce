package pl.edu.mimuw.mapreduce.network;

import pl.edu.mimuw.proto.common.ProcessType;

import java.net.InetSocketAddress;

/* Every operation should be atomic. */
public interface NetworkInfo {
    /** Adds a sockAddr to a database of available processes. */
    void add(ProcessType type, InetSocketAddress sockAddr);

    /** Removes a sockAddr from a database of available processes. */
    void remove(InetSocketAddress sockAddr);

    /** Gets InetSocketAddress of a process with type in a round-robin fashion. */
    InetSocketAddress get(ProcessType type);
}
