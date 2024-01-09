package pl.edu.mimuw.mapreduce.worker;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    public void run() throws ExecutionException, InterruptedException {
        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (Iterator<FileRep> it = storage.getSplitIterator(dataDir, split); it.hasNext(); ) {
            FileRep fr = it.next();
            futures.add(pool.submit(new FileProcessor(fr)));
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

        FileProcessor(FileRep fr) {
            this.fr = fr;
        }

        @Override
        public Void call() throws IOException, InterruptedException {
            var inputFile = fr.file();
            var outputFile = new File(tempDir, String.valueOf(fr.id()));
            var files = new File[]{inputFile, outputFile};

            var pb = new ProcessBuilder();
            var n = binaries.size();

            for (long i = 0; i < n; i++) {
                pb.redirectInput(files[(int) (i % 2)]);
                pb.redirectOutput(files[(int) (1 - i % 2)]);
                pb.command(binaries.get(i).getAbsolutePath());
                pb.start().waitFor();
            }

            storage.putFile(destinationId, fr.id(), files[n - 1]);
            return null;
        }
    }
}
