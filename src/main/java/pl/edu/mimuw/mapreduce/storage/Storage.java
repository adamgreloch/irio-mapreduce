package pl.edu.mimuw.mapreduce.storage;

import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.util.Iterator;

public interface Storage {
    /* Storage can organize normal files (mapreduce data) in flat directories. */

    /** Retrieves a file named file_id from a directory dir_id */
    FileRep get_file(long dir_id, long file_id);

    /** Puts a file named file_id to a directory dir_id */
    void put_file(long dir_id, File file);

    /** Gets an iterator over files from a split of directory dir_id */
    Iterator<FileRep> get_split_iterator(long dir_id, Split split);

    /* Binaries are stored in a space isolated from normal files. */

    /** Retrieves executable binary by its bin_id */
    File get_binary(long bin_id);

    /** Puts executable binary to storage under bin_id */
    void put_binary(long bin_id, File file);
}
