package pl.edu.mimuw.mapreduce.worker.util;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TaskProcessor implements AutoCloseable {
    protected final String dataDir;
    protected final String destDirId;
    protected final ConcurrentHashMap<Long, File> binaries;
    protected final List<Long> binIds;
    protected final Path tempDir;
    protected final Storage storage;

    public TaskProcessor(Storage storage, List<Long> binIds, String dataDir,
                           String destDirId) throws IOException {
        this.dataDir = dataDir;
        this.destDirId = destDirId;
        this.storage = storage;
        this.tempDir = Files.createTempDirectory("processor_" + UUID.randomUUID());
        this.binaries = new ConcurrentHashMap<>();
        this.binIds = binIds;
        for (var binId : binIds)
            this.binaries.put(binId, storage.getFile(Storage.BINARY_DIR, binId).file());
    }

    public File copyInputFileToTempDir(FileRep fr) throws IOException {
        return Files.copy(fr.file().toPath(), tempDir.resolve(String.valueOf(fr.id()))).toFile();
    }

    @Override
    public void close() throws IOException {
        for (var binary : binaries.values())
            Files.delete(binary.toPath().toAbsolutePath());
    }
}
