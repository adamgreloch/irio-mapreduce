package pl.edu.mimuw.mapreduce.storage.local;

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


/* NOTE: We assume that the argument-directories already exist */
public class DistrStorage implements Storage {
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

            Utils.LOGGER.info("Hooked up DistrStorage to path successfully " + storagePathString);

            this.tmpStorage = Files.createTempDirectory("storage_tmp").toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path getStoragePath() {
        return storagePath;
    }

    @Override
    public FileRep getFile(String dirId, long fileId) {
        Path dirPath = storagePath.resolve(dirId);
        Path filePath = dirPath.resolve(String.valueOf(fileId));
        File file = new File(filePath.toString());
        if (!file.exists()) {
            throw new IllegalStateException("File does not exist: " + file);
        }
        File fileCopy;
        try {
            fileCopy = Files.copy(file.toPath(), Paths.get((tmpStorage.resolve(dirPath).resolve(filePath)).toString()))
                            .toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new LocalFileRep(fileCopy, fileId);
    }

    @Override
    public FileRep getFile(Path path) {
        File file = new File(path.toString());
        if (!file.exists()) {
            throw new IllegalStateException("File does not exist: " + file);
        }
        try {
            Files.copy(file.toPath(), Paths.get((tmpStorage.resolve(path)).toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new LocalFileRep(file, Long.parseLong(path.getFileName().toString()));
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
            Files.copy(file.toPath(), Paths.get((storagePath.resolve(dirId)).resolve(String.valueOf(fileId)).toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putReduceFile(String dirId, long fileId, String authorId, File file) {
        try {
            createDir(dirId);
            Files.copy(file.toPath(), Paths.get((storagePath.resolve(dirId)).resolve(fileId + "_R_" + authorId).toString()));
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
            public boolean hasNext() {return current <= split.getEnd();} // NOTE: end bound is inclusive

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
        createDir(dirId);
        Set<String> fileNamesPrefixes = new HashSet<>();
        try(Stream<Path> files = Files.list(Paths.get(storagePath.resolve(dirId).toString()))) {
            files.forEach(path -> {
                String fileName = path.getFileName().toString();
                int firstUnderscoreIndex = fileName.indexOf("_");
                int secondUnderscoreIndex = fileName.indexOf("_", firstUnderscoreIndex + 1);
                String fileNamePrefix = fileName.substring(0, secondUnderscoreIndex);
                if (fileNamesPrefixes.contains(fileNamePrefix)) {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot remove reduce duplicates in dir: " + dirId);
                    }
                } else {
                    fileNamesPrefixes.add(fileNamePrefix);
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException("Cannot remove reduce duplicates in dir: " + dirId);
        }
    }
}

