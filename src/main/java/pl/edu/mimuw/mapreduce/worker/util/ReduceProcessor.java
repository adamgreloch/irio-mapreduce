package pl.edu.mimuw.mapreduce.worker.util;

import pl.edu.mimuw.mapreduce.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ReduceProcessor extends TaskProcessor {
    private final long fileId;

    public ReduceProcessor(Storage storage, long fileId, List<Long> binIds, String dataDir,
                           String destDirId) throws IOException {
        super(storage, binIds, dataDir, destDirId);
        this.fileId = fileId;
    }

    public void reduce() throws ExecutionException, InterruptedException, IOException {
        var inputFile = storage.getFile(dataDir, fileId).file();
        var outputFile = new File(tempDir, String.valueOf(fileId));
        var files = new File[]{inputFile, outputFile};

        var pb = new ProcessBuilder();
        var i = 0;

        for (var binId : binIds) {
            var binary = binaries.get(binId).getAbsolutePath();

            var inputPath = files[i % 2].getAbsolutePath();
            String outputPath = files[1 - i % 2].getAbsolutePath();

            pb.command(binary,
                    "-i " + inputPath,
                    "-o " + outputPath);

            pb.start().waitFor();

            i++;
        }

        storage.putFile(String.valueOf(destDirId), fileId, files[binIds.size() % 2]);
    }
}
