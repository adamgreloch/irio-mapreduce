package pl.edu.mimuw.mapreduce.network;

import pl.edu.mimuw.proto.common.ProcessType;

import java.net.InetSocketAddress;

public interface SystemInfo {
    /** Gets InetSocketAddress of a process with type in round-robin fashion. */
    InetSocketAddress get(ProcessType type);
}
