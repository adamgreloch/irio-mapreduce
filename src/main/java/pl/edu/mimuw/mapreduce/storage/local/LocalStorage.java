package pl.edu.mimuw.mapreduce.storage.local;

import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


// TODO implements Storage
public class LocalStorage {
    private final List<LocalFileRep> fileList;
    private final long largestId;

    public LocalStorage() {
        this.fileList = new ArrayList<>();
        largestId = 0;
    }

    public LocalFileRep get(long id) {
        for (LocalFileRep localFileRep : fileList) {
            if (localFileRep.id() == id) {
                return localFileRep;
            }
        }
        return null;
    }

    public void put(File file) {
        LocalFileRep localFileRep = new LocalFileRep(file, largestId + 1);
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (largestId == Long.MAX_VALUE) {
            throw new IllegalStateException("Cannot add more files");
        }
        fileList.add(localFileRep);
        fileList.sort((a, b) -> (int) (a.id() - b.id()));
    }

    private int findIndexOfId(long id) {
        for (int i = 0; i < fileList.size(); i++) {
            if (fileList.get(i).id() == id) {
                return i;
            }
        }
        return -1;
    }

    public Iterator<LocalFileRep> get_split_iterator(Split split) {
        return new Iterator<LocalFileRep>() {
            private int current = findIndexOfId(split.getBeg());

            @Override
            public boolean hasNext() {
                return current < split.getEnd();
            }

            @Override
            public LocalFileRep next() {
                return fileList.get(current++);
            }
        };
    }

    public static void main(String[] args) {
        // Example usage
        LocalStorage storage = new LocalStorage();
        storage.put(new File("file1"));
        storage.put(new File("file2"));
        storage.put(new File("file3"));
        System.out.println(storage.get(1).file().getName());
        System.out.println(storage.get(2).file().getName());
        System.out.println(storage.get(3).file().getName());
    }
}

