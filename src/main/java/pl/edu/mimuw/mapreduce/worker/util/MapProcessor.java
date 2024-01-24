package pl.edu.mimuw.mapreduce.worker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class MapProcessor extends TaskProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapProcessor.class);

    private final Split split;
    private final ExecutorService pool;
    private final Semaphore mutex = new Semaphore(1);
    private List<Future<Void>> futures = new ArrayList<>();
    private final int rNum;

    public MapProcessor(Storage storage, ExecutorService pool, Split split, List<Long> binIds, String dataDir, String destDirId, int rNum)
            throws IOException {
        super(storage, binIds, dataDir, destDirId);
        this.split = split;
        this.rNum = rNum;
        this.pool = pool;
    }

    public void map() throws ExecutionException, InterruptedException {
        for (Iterator<Path> it = storage.getSplitIterator(String.valueOf(dataDir), split); it.hasNext(); ) {
            Path path = it.next();
            futures.add(pool.submit(new FileProcessor(storage.getFile(path), binIds.size())));
        }
        for (var future : futures)
            future.get();
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
            var inputFile = copyInputFileToTempDir(fr);
            var outputFile = Files.createFile(tempDir.resolve(fr.id() + "_2")).toFile();
            var files = new File[]{inputFile, outputFile};
            var pb = new ProcessBuilder();
            var i = 0;

            for (var binId : binIds) {
                var binary = binaries.get(binId).getAbsolutePath();
                var inputPath = files[i % 2].getAbsolutePath();
                String outputPath;
                if (i == binaryCount - 1) {
                    // Partition phase. The output path is just a destination directory
                    LOGGER.info("Executing partition binary " + binary + " on file " + inputPath);
                    outputPath = storage.getDirPath(String.valueOf(destDirId)).toAbsolutePath().toString();
                    pb.command(binary, "-R", String.valueOf(rNum), "-i", inputPath, "-o", outputPath);
                    mutex.acquire();
                    pb.inheritIO().start().waitFor();
                    mutex.release();
                    LOGGER.info("Partition phase finished on file: " + inputPath);
                    LOGGER.info("Partition results are on path: " + outputPath);
                } else {
                    LOGGER.info("Executing map binary " + binary + " on file " + inputPath);
                    outputPath = files[1 - i % 2].getAbsolutePath();
                    pb.command(binary, "-i", inputPath, "-o", outputPath);
                    pb.inheritIO().start().waitFor();
                    LOGGER.info("Map binary execution finished on file: " + inputPath);
                }
                i++;
            }

            LOGGER.info("Map/Partition task finished for input file " + dataDir + "/" + fr.id());

            return null;
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Waiting for threads to finish");
        for (var future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        super.close();
    }
}
