package pl.edu.mimuw.mapreduce.storage.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


/* NOTE: We assume that the argument-directories already exist */
public class DistrStorage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistrStorage.class);

    private final Path storagePath;
    private Path tmpStorage;

    public DistrStorage(String storagePathString) {
        this.storagePath = Paths.get(storagePathString);
        try {
            if (!Files.exists(this.storagePath)) {
                throw new IOException("Storage path doesn't exist: " + this.storagePath);
            }

            if (!storagePath.toFile().canRead() || !storagePath.toFile().canWrite()) {
                throw new IOException("Cannot read/write to storage path: " + this.storagePath);
            }

            Files.createDirectories(this.storagePath.resolve(BINARY_DIR));
            Files.createDirectories(this.storagePath.resolve(STATE_DIR));

            LOGGER.info("Hooked up DistrStorage to path successfully " + storagePathString);

            this.tmpStorage = Files.createTempDirectory("storage_tmp").toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path getStoragePath() {
        return storagePath;
    }

    @Override
    public File getBinary(long fileId) {
        var binPath = storagePath.resolve(BINARY_DIR).resolve(String.valueOf(fileId));
        LOGGER.info("Accessing binary " + fileId + " under " + binPath);
        return binPath.toFile();
    }

    @Override
    public FileRep getFile(String dirId, long fileId) {
        return getFile(storagePath.resolve(dirId).resolve(String.valueOf(fileId)));
    }

    @Override
    public FileRep getFile(Path path) {
        try {
            if (!Files.exists(path)) {
                throw new IllegalStateException("File does not exist under path: " + path);
            }
            var fileId = path.getFileName().toString();
            var tmpDirPath = tmpStorage.resolve(path.getParent().getFileName());
            Files.createDirectories(tmpDirPath);
            Path copyPath = Files.copy(path, tmpDirPath.resolve(fileId), COPY_ATTRIBUTES, REPLACE_EXISTING);
            LOGGER.info("Downloaded file " + path + " to local " + copyPath);
            return new LocalFileRep(copyPath.toFile(), Long.parseLong(fileId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Path getDirPath(String dirId) {
        return storagePath.resolve(dirId);
    }

    @Override
    public void createDir(String dirId) {
        var dirPath = storagePath.resolve(dirId);
        if (!Files.exists(dirPath)) {
            try {
                Files.createDirectory(dirPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void putFile(String dirId, long fileId, File file) {
        try {
            createDir(dirId);
            Files.copy(file.toPath(), Paths.get((storagePath.resolve(dirId)).resolve(String.valueOf(fileId))
                                                                            .toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putReduceFile(String dirId, long fileId, String authorId, File file) {
        try {
            createDir(dirId);
            Files.copy(file.toPath(), getDirPath(dirId).resolve(fileId + "_R_" + authorId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getFileCount(String dirId) {
        long length = 0;
        try (Stream<Path> files = Files.list(Paths.get(storagePath.resolve(dirId).toString()))) {
            length = files.count();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot count files in dir: " + dirId);
        }
        return length;
    }

    @Override
    public List<Split> getSplitsForDir(String dirId, int splits) {
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
    public Iterator<Path> getSplitIterator(String dirId, Split split) {
        return new Iterator<Path>() {
            private long current = split.getBeg();

            @Override
            public boolean hasNext() {
                return current <= split.getEnd();
            } // NOTE: end bound is inclusive

            @Override
            public Path next() {
                return storagePath.resolve(dirId).resolve(String.valueOf(current++));
            }
        };
    }

    @Override
    public Iterator<Path> getDirIterator(String dirId) {
        return new Iterator<Path>() {
            private long current = 0;

            @Override
            public boolean hasNext() {
                return current < getFileCount(dirId);
            }

            @Override
            public Path next() {
                return storagePath.resolve(dirId).resolve(String.valueOf(current++));
            }
        };
    }

    @Override
    public void saveState(String podId, String state) {
        createDir("STATE_DIR");
        try {
            Files.writeString(storagePath.resolve("STATE_DIR").resolve(podId), state);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String retrieveState(String podId) {
        String state;
        try {
            state = Files.readString(storagePath.resolve("STATE_DIR").resolve(podId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return state;
    }

    @Override
    public void removeReduceDuplicates(String dirId) {
        Set<String> fileNamesPrefixes = new HashSet<>();
        try (Stream<Path> stream = Files.list(storagePath.resolve(dirId))) {
            List<Path> files = stream.toList();
            for (Path path : files) {
                String fileName = path.getFileName().toString();
                int firstUnderscoreIndex = fileName.indexOf("_");
                String fileNamePrefix = fileName.substring(0, firstUnderscoreIndex);
                if (fileNamesPrefixes.contains(fileNamePrefix)) {
                    Files.delete(path);
                } else {
                    fileNamesPrefixes.add(fileNamePrefix);
                    Files.move(path, path.getParent().resolve(fileNamePrefix));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not reduce duplicates: " + e);
        }
    }

    @Override
    public void close() throws Exception {
        Utils.removeDirRecursively(this.tmpStorage);
    }
}

