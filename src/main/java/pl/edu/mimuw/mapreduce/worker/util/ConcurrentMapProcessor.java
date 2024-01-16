package pl.edu.mimuw.mapreduce.worker.util;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class ConcurrentMapProcessor implements AutoCloseable {
    private final long dataDir;
    private final long destDirId;
    private final ConcurrentHashMap<Long, File> binaries;
    private final List<Long> binIds;
    private final String tempDir;
    private final Storage storage;
    private final Split split;

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    public ConcurrentMapProcessor(Storage storage, Split split, List<Long> binIds, long dataDir,
                                  long destDirId) throws IOException {
        this.dataDir = dataDir;
        this.destDirId = destDirId;
        this.split = split;
        this.storage = storage;
        this.tempDir = Files.createTempDirectory("processor_" + dataDir + "_" + split.getBeg() +
                "_" + split.getEnd()).toFile().getAbsolutePath();
        this.binaries = new ConcurrentHashMap<>();
        this.binIds = binIds;
        for (var binId : binIds)
            this.binaries.put(binId, storage.getFile(Storage.BINARY_DIR, binId).file());
    }

    public void map() throws ExecutionException, InterruptedException {
        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (Iterator<Path> it = storage.getSplitIterator(dataDir, split); it.hasNext(); ) {
            Path path = it.next();
            futures.add(pool.submit(new FileProcessor(storage.getFile(path), binaries.size())));
        }
        for (var future : futures)
            future.get();
    }

    @Override
    public void close() throws IOException {
        for (var binary : binaries.values())
            Files.delete(binary.toPath().toAbsolutePath());
    }

    private class FileProcessor implements Callable<Void> {
        private final FileRep fr;
        private final long binaryCount;

        FileProcessor(FileRep fr, long binaryCount) {
            this.fr = fr;
            this.binaryCount = binaryCount;
        }

        @Override
        public Void call() throws IOException, InterruptedException {
            var inputFile = fr.file();
            var outputFile = new File(tempDir, String.valueOf(fr.id()));
            var files = new File[]{inputFile, outputFile};
            var pb = new ProcessBuilder();
            var i = 0;

            for (var binId : binIds) {
                var binary = binaries.get(binId).getAbsolutePath();
                var inputPath = files[i % 2].getAbsolutePath();
                String outputPath;
                if (i == binaryCount - 1) {
                    // Partition phase. The output path is just a destination directory
                    outputPath = storage.getDirPath(destDirId).toAbsolutePath().toString();
                } else {
                    outputPath = files[1 - i % 2].getAbsolutePath();
                }
                pb.command(binary,
                        "-i " + inputPath,
                        "-o " + outputPath);
                pb.start().waitFor();
                i++;
            }

            storage.putFile(destDirId, fr.id(), files[(int) binaryCount]);
            return null;
        }
    }
}
