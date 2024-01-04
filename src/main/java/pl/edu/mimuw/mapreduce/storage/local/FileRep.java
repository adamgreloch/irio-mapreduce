package pl.edu.mimuw.mapreduce.storage.local;

import java.io.File;

public class FileRep {
    private File file;
    private long id;

    // Constructor
    public FileRep(File file, long id) {
        this.file = file;
        this.id = id;
    }

    // Method to get the file
    public File getFile() {
        return file;
    }

    // Method to get the id
    public long getId() {
        return id;
    }
}
