package pl.edu.mimuw.mapreduce.storage.local;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;


/* NOTE: We assume that the argument-directories already exist */
public class DistrStorage implements Storage {
    @Override
    public FileRep getFile(long dirId, long fileId) {
        File file = new File(String.valueOf(dirId) + "/" + String.valueOf(fileId));
        if (!file.exists()) {
            throw new IllegalStateException("File does not exist");
        }
        return new LocalFileRep(file, fileId);
    }

    @Override
    public void putFile(long dirId, long fileId, File file) {
        try {
            Files.move(file.toPath(), Paths.get(String.valueOf(dirId) + "/" + String.valueOf(fileId)), ATOMIC_MOVE);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot move file atomically");
        }
    }

    @Override
    public long getFileCount(long dirId) {
        long length = 0;
        File directory = new File(String.valueOf(dirId));
        try (Stream<Path> files = Files.list(Paths.get(String.valueOf(dirId)))) {
            length = files.count();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot count files");
        }
        return length;
    }

    @Override
    public List<Split> getSplitsForDir(long dirId, int splits) {
        List<Split> splitList = new ArrayList<>();
        long fileCount = getFileCount(dirId);
        long splitSize = fileCount / splits;
        long current = 0;
        for (int i = 0; i < splits; i++) {
            if (splits >= 2 && i == splits - 1 && fileCount % splits != 0) {
                // add the rest of the files to this split
                splitList.add(new SplitBuilder(current, fileCount - 1).build());
            } else {
                splitList.add(new SplitBuilder(current, current + splitSize - 1).build());
                current += splitSize;
            }
        }
        return splitList;
    }

    @Override
    public Iterator<FileRep> getSplitIterator(long dirId, Split split) {
        return new Iterator<FileRep>() {
            private long current = split.getBeg();

            @Override
            public boolean hasNext() {return current <= split.getEnd();} // NOTE: end bound is inclusive

            @Override
            public FileRep next() {
                return getFile(dirId, current++);
            }
        };
    }

    @Override
    public Iterator<FileRep> getDirIterator(long dirId) {
        return new Iterator<FileRep>() {
            private long current = 0;

            @Override
            public boolean hasNext() {
                return current < getFileCount(dirId);
            }

            @Override
            public FileRep next() {
                return getFile(dirId, current++);
            }
        };
    }
}

