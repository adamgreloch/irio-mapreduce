package pl.edu.mimuw.mapreduce.worker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TaskProcessor implements AutoCloseable {
    protected final String dataDir;
    protected final String destDirId;
    protected static ConcurrentHashMap<Long, File> binaries = new ConcurrentHashMap<>();
    protected final List<Long> binIds;
    protected final Path tempDir;
    protected final Storage storage;
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskProcessor.class);
    private static final AtomicInteger internalId = new AtomicInteger(0);

    public TaskProcessor(Storage storage, List<Long> binIds, String dataDir, String destDirId) throws IOException {
        LOGGER.info("Initializing task processor");
        this.dataDir = dataDir;
        this.destDirId = destDirId;
        this.storage = storage;
        this.tempDir = ClusterConfig.TEMP_DIR.resolve("processor-" + internalId.getAndIncrement());
        Files.createDirectories(tempDir);
        this.binIds = binIds;
        for (var binId : binIds) {
            binaries.computeIfAbsent(binId, (id) -> storage.getFile(Storage.BINARY_DIR, id).file());
        }
    }

    public File copyInputFileToTempDir(FileRep fr) throws IOException {
        return Files.copy(fr.file().toPath(), tempDir.resolve(String.valueOf(fr.id()))).toFile();
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing task processor");
        Utils.removeDirRecursively(this.tempDir);
    }
}
