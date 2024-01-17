package pl.edu.mimuw.mapreduce.storage;

import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public interface Storage {
    /* Storage can organize normal files (mapreduce data) in flat directories. */

    /* Reserved directory levels */
    long BINARY_DIR = 0;
    long MAPREDUCE_MIN = BINARY_DIR + 1;

    /** Retrieves a file with id fileId from a directory dirId */
    FileRep getFile(long dirId, long fileId);

    /** Retrieves a file with id fileId from a directory dirId */
    FileRep getFile(String dirId, long fileId);

    FileRep getFile(Path path);

    Path getDirPath(long dirId);

    /** Puts a file with id fileId to a directory dirId */
    void putFile(long dirId, long fileId, File file);

    /** Puts a file with id fileId to a directory dirId */
    void putFile(String dirId, long fileId, File file);

    /** Gets a number of files in directory dirId */
    long getFileCount(long dirId);

    /** Splits a directory id range into equal splits */
    List<Split> getSplitsForDir(String dirId, int splits);

    /** Gets an iterator over files from a split of directory dirId */
    Iterator<Path> getSplitIterator(long dirId, Split split);

     /** Gets an iterator over all files in a directory dirId */
    Iterator<Path> getDirIterator(long dirId);
}
