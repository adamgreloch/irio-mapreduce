package pl.edu.mimuw.mapreduce.storage;

import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.util.Iterator;

public interface Storage {
    /* Storage can organize normal files (mapreduce data) in flat directories. */

    /* Reserved directory levels */
    long BINARY_DIR = 0;
    long MAPREDUCE_MIN = BINARY_DIR + 1;

    /** Retrieves a file with id fileId from a directory dirId */
    FileRep getFile(long dirId, long fileId);

    /** Puts a file with id fileId to a directory dirId */
    void putFile(long dirId, long fileId, File file);

    /** Gets a number of files in directory dirId */
    long getFileCount(long dirId);

    /** Gets an iterator over files from a split of directory dirId */
    Iterator<FileRep> getSplitIterator(long dirId, Split split);

     /** Gets an iterator over all files in a directory dirId */
    Iterator<FileRep> getDirIterator(long dirId);
}
