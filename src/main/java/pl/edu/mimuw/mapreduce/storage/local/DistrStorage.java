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
    private final Path storagePath;

    public DistrStorage(String storagePathString) {
        this.storagePath = Paths.get(storagePathString);
    }

    public Path getStoragePath() {
        return storagePath;
    }

    @Override
    public FileRep getFile(long dirId, long fileId) {
        Path dirPath = storagePath.resolve(String.valueOf(dirId));
        Path filePath = dirPath.resolve(String.valueOf(fileId));
        File file = new File(filePath.toString());
        if (!file.exists()) {
            throw new IllegalStateException("File does not exist");
        }
        return new LocalFileRep(file, fileId);
    }

    @Override
    public FileRep getFile(Path path) {
        File file = new File(path.toString());
        if (!file.exists()) {
            throw new IllegalStateException("File does not exist");
        }
        return new LocalFileRep(file, Long.parseLong(path.getFileName().toString()));
    }

    @Override
    public void putFile(long dirId, long fileId, File file) {
        try {
            Files.move(file.toPath(), Paths.get((storagePath.resolve(String.valueOf(dirId))).resolve(String.valueOf(fileId)).toString()), ATOMIC_MOVE);
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
    public Iterator<Path> getSplitIterator(long dirId, Split split) {
        return new Iterator<Path>() {
            private long current = split.getBeg();

            @Override
            public boolean hasNext() {return current <= split.getEnd();} // NOTE: end bound is inclusive

            @Override
            public Path next() {
                return storagePath.resolve(String.valueOf(dirId)).resolve(String.valueOf(current++));
            }
        };
    }

    @Override
    public Iterator<Path> getDirIterator(long dirId) {
        return new Iterator<Path>() {
            private long current = 0;

            @Override
            public boolean hasNext() {
                return current < getFileCount(dirId);
            }

            @Override
            public Path next() {
                return storagePath.resolve(String.valueOf(dirId)).resolve(String.valueOf(current++));
            }
        };
    }
}

