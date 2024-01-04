package pl.edu.mimuw.mapreduce.storage.local;

import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



public class LocalStorage {
    private final List<FileRep> fileList;
    private final long largestId;

    public LocalStorage() {
        this.fileList = new ArrayList<>();
        largestId = 0;
    }

    public FileRep get(long id) {
        for (FileRep fileRep : fileList) {
            if (fileRep.getId() == id) {
                return fileRep;
            }
        }
        return null;
    }

    public void put(File file) {
        FileRep fileRep = new FileRep(file, largestId + 1);
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (largestId == Long.MAX_VALUE) {
            throw new IllegalStateException("Cannot add more files");
        }
        fileList.add(fileRep);
        fileList.sort((a, b) -> (int) (a.getId() - b.getId()));
    }

    public int findIndexOfId(long id) {
        for (int i = 0; i < fileList.size(); i++) {
            if (fileList.get(i).getId() == id) {
                return i;
            }
        }
        return -1;
    }

    public Iterator<FileRep> get_split_iterator(Split split) {
        return new Iterator<FileRep>() {
            private int current = findIndexOfId(split.getBeg());

            @Override
            public boolean hasNext() {
                return current < split.getEnd();
            }

            @Override
            public FileRep next() {
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
        System.out.println(storage.get(1).getFile().getName());
        System.out.println(storage.get(2).getFile().getName());
        System.out.println(storage.get(3).getFile().getName());
    }
}

