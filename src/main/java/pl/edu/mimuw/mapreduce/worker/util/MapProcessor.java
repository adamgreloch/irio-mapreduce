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

public class MapProcessor extends TaskProcessor {
    private final Split split;
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    private final Semaphore mutex = new Semaphore(1);

    public MapProcessor(Storage storage, Split split, List<Long> binIds, String dataDir,
                        String destDirId) throws IOException {
        super(storage, binIds, dataDir, destDirId);
        this.split = split;
    }

    public void map() throws ExecutionException, InterruptedException {
        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (Iterator<Path> it = storage.getSplitIterator(String.valueOf(dataDir), split); it.hasNext(); ) {
            Path path = it.next();
            futures.add(pool.submit(new FileProcessor(storage.getFile(path), binaries.size())));
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
                    outputPath = storage.getDirPath(String.valueOf(destDirId)).toAbsolutePath().toString();
                    pb.command(binary,
                            "-R", "1", // TODO
                            "-i", inputPath,
                            "-o", outputPath);
                    mutex.acquire();
                    pb.inheritIO().start().waitFor();
                    mutex.release();
                } else {
                    outputPath = files[1 - i % 2].getAbsolutePath();
                    pb.command(binary,
                            "-i", inputPath,
                            "-o", outputPath);
                    pb.inheritIO().start().waitFor();
                }
                i++;
            }

//            storage.putFile(String.valueOf(destDirId), fr.id(), files[(int) (1 - binaryCount % 2)]);
            return null;
        }
    }
}
