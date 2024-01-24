package pl.edu.mimuw.mapreduce.storage;

import java.io.File;

public interface FileRep {

    /**
     * Method to access the file
     */
    File file();

    /**
     * Gets file id
     */
    long id();
}
