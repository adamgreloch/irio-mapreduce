package pl.edu.mimuw.mapreduce.worker.util;

import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
        var fr = storage.getFile(dataDir, fileId);
        var inputFile = copyInputFileToTempDir(fr);
        var outputFile = Files.createFile(tempDir.resolve(fr.id() + "_2")).toFile();
        var files = new File[]{inputFile, outputFile};

        var pb = new ProcessBuilder();
        var i = 0;

        for (var binId : binIds) {
            var binary = binaries.get(binId).getAbsolutePath();

            var inputPath = files[i % 2].getAbsolutePath();
            String outputPath = files[1 - i % 2].getAbsolutePath();

            pb.command(binary,
                    "-i", inputPath,
                    "-o", outputPath + "_R_" + ClusterConfig.POD_NAME);

            pb.start().waitFor();

            i++;
        }

        storage.putFile(String.valueOf(destDirId), fileId, files[binIds.size() % 2]);
    }
}
