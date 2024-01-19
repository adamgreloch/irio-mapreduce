package pl.edu.mimuw.mapreduce.storage;

import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public interface Storage {
    /* Storage can organize normal files (mapreduce data) in flat directories. */

    /* Reserved directory levels */
    String BINARY_DIR = "BINARY_0";

    /** Retrieves a file with id fileId from a directory dirId */
    FileRep getFile(String dirId, long fileId);

    FileRep getFile(Path path);

    Path getDirPath(String dirId);

    void createDir(String dirId);

    /** Puts a file with id fileId to a directory dirId */
    void putFile(String dirId, long fileId, File file);

    /** Gets a number of files in directory dirId */
    long getFileCount(String dirId);

    /** Splits a directory id range into equal splits */
    List<Split> getSplitsForDir(String dirId, int splits);

    /** Gets an iterator over files from a split of directory dirId */
    Iterator<Path> getSplitIterator(String dirId, Split split);

     /** Gets an iterator over all files in a directory dirId */
    Iterator<Path> getDirIterator(String dirId);
}
