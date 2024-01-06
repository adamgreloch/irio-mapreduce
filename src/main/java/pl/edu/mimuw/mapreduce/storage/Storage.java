package pl.edu.mimuw.mapreduce.storage;

import pl.edu.mimuw.proto.common.ProcessType;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.util.Iterator;

public interface Storage {
    /* Storage can organize normal files (mapreduce data) in flat directories. */

    /* Reserved directory levels */
    long NETWORK_DIR_MIN = 0;
    long NETWORK_DIR_MAX = ProcessType.values().length - 1;
    long BINARY_DIR = NETWORK_DIR_MAX + 1;
    long MAPREDUCE_MIN = BINARY_DIR + 1;

    /** Retrieves a file with id fileId from a directory dirId */
    FileRep getFile(long dirId, long fileId);

    /** Puts a file with id fileId to a directory dirId */
    void putFile(long dirId, long fileId, File file);

    /**
     * Puts a file named filename with content to a directory dirId. File id
     * is chosen arbitrarily.
     */
    void putFile(long dirId, String filename, String content);

    /** Gets a filename of a file with id fileId in a directory dirId */
    String getFileName(long dirId, long fileId);

    /** Updates a file named filename in directory dirId to contain newContent */
    void updateFile(long dirId, String filename, String newContent);

    /** Removes a file named filename from a directory dirId */
    void removeFile(long dirId, String filename);

    /** Gets a number of files in directory dirId */
    long getFileCount(long dirId);

    /** Gets an iterator over files from a split of directory dirId */
    Iterator<FileRep> getSplitIterator(long dirId, Split split);

     /** Gets an iterator over all files in a directory dirId */
    Iterator<FileRep> getDirIterator(long dirId);
}
