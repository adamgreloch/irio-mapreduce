package pl.edu.mimuw.mapreduce.network;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.ProcessType;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A NetworkInfo implementation using stable storage and caching
 */
public class StableNetworkInfo implements NetworkInfo {
    private static final String sep = "_";

    private record DataPair(ProcessType type, InetSocketAddress sockAddr) {
    }

    private final Storage storage;
    private final ConcurrentHashMap<ProcessType, ArrayList<InetSocketAddress>> typeToSockAddr = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetSocketAddress, ProcessType> sockAddrToType = new ConcurrentHashMap<>();

    /** Generates a random number in a range [min, max) */
    public long getRandomNumber(long min, long max) {
        return (long) ((Math.random() * (max - min)) + min);
    }

    private String serializeData(InetSocketAddress addr) {
        return addr.getHostString() + sep + addr.getPort();
    }

    private InetSocketAddress deserializeSockAddr(String string) {
        String[] tokens = string.split(sep);
        return new InetSocketAddress(tokens[1], Integer.parseInt(tokens[2]));
    }

    private void addAddr(ProcessType type, InetSocketAddress sockAddr) {
        typeToSockAddr.get(type).add(sockAddr);
        sockAddrToType.put(sockAddr, type);
    }

    private void removeAddr(ProcessType type, InetSocketAddress sockAddr) {
        typeToSockAddr.get(type).remove(sockAddr);
        sockAddrToType.remove(sockAddr);
    }

    private void restoreState() {
        for (var type : ProcessType.values()) {
            assert type.getNumber() <= Storage.NETWORK_DIR_MAX;
            for (Iterator<FileRep> it = storage.getDirIterator(type.getNumber()); it.hasNext(); ) {
                var fr = it.next();
                var filename = fr.file().getName();
                var sockAddr = deserializeSockAddr(filename);

                addAddr(type, sockAddr);
            }
        }
    }

    public StableNetworkInfo(Storage storage) {
        for (var type : ProcessType.values()) typeToSockAddr.put(type, new ArrayList<>());
        this.storage = storage;
        restoreState();
    }

    @Override
    public void add(ProcessType type, InetSocketAddress sockAddr) {
        assert type.getNumber() <= Storage.NETWORK_DIR_MAX;
        storage.putFile(type.getNumber(), serializeData(sockAddr), "");
        addAddr(type, sockAddr);
    }

    @Override
    public void remove(InetSocketAddress sockAddr) {
        var type = sockAddrToType.get(sockAddr);
        storage.removeFile(type.getNumber(), serializeData(sockAddr));
        removeAddr(type, sockAddr);
    }

    @Override
    public InetSocketAddress get(ProcessType type) {
        var max = storage.getFileCount(type.getNumber());
        return deserializeSockAddr(storage.getFileName(type.getNumber(), getRandomNumber(0, max)));
    }
}
