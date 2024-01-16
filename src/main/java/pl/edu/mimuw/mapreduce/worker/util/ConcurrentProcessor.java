package pl.edu.mimuw.mapreduce.worker.util;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class ConcurrentProcessor implements AutoCloseable {
    private final long dataDir;
    private final long destinationId;
    private final ConcurrentHashMap<Long, File> binaries;
    private final String tempDir;
    private final Storage storage;
    private final Split split;

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    public ConcurrentProcessor(Storage storage, Split split, List<Long> binIds, long dataDir, long destinationId) throws IOException {
        this.dataDir = dataDir;
        this.destinationId = destinationId;
        this.split = split;
        this.storage = storage;
        this.tempDir =
                Files.createTempDirectory("processor_" + dataDir + "_" + split.getBeg() + "_" + split.getEnd()).toFile().getAbsolutePath();
        this.binaries = new ConcurrentHashMap<>();
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

    public void reduce() throws ExecutionException, InterruptedException {
        ArrayList<Future<Void>> futures = new ArrayList<>();
        // Reduce phase
        for (Iterator<Path> it = storage.getSplitIterator(dataDir, split); it.hasNext(); ) {
            Path path = it.next();
            futures.add(pool.submit(new FileProcessor(storage.getFile(path), 1)));
        }
        for (var future : futures)
            future.get();
        futures.clear();

        // Combine phase
        Queue<SplitBuilder> queue = new LinkedList<>();

        var combiner = new Combiner(storage, dataDir, destinationId, this.binaries.get(1L));

        for (long fileId = split.getBeg(); fileId < split.getEnd(); fileId++)
            queue.add(new SplitBuilder(fileId, fileId + 1));

        while (queue.size() > 1) {
            var phaseSize = queue.size();
            if (phaseSize % 2 != 0) phaseSize--;

            for (int i = 0; i < phaseSize; i += 2) {
                var s1 = queue.poll();
                var s2 = queue.poll();

                futures.add(pool.submit(() -> {
                    combiner.combine(s1, s2);
                    return null;
                }));

                assert s2 != null;
                queue.add(SplitBuilder.merge(s1, s2));
            }

            for (var future : futures)
                future.get();
            futures.clear();
        }
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

            for (long i = 0; i < binaryCount + 1; i++) {
                pb.redirectInput(files[(int) (i % 2)]);
                pb.redirectOutput(files[(int) (1 - i % 2)]);
                pb.command(binaries.get(i).getAbsolutePath());
                pb.start().waitFor();
            }

            storage.putFile(destinationId, fr.id(), files[(int) binaryCount]);
            return null;
        }
    }
}
