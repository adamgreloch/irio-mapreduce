package pl.edu.mimuw.mapreduce.worker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ReduceProcessor extends TaskProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReduceProcessor.class);
    private final long fileId;

    public ReduceProcessor(Storage storage, long fileId, List<Long> binIds, String dataDir,
                           String destDirId) throws IOException {
        super(storage, binIds, dataDir, destDirId);
        this.fileId = fileId;
    }

    public void reduce() throws ExecutionException, InterruptedException, IOException {
        var inputFile = storage.getFile(dataDir, fileId);
        var outputFile = Files.createFile(tempDir.resolve(inputFile.id() + "_2")).toFile();
        var files = new File[]{inputFile.file(), outputFile};

        var pb = new ProcessBuilder();
        var i = 0;

        for (var binId : binIds) {
            var binary = binaries.get(binId).getAbsolutePath();

            var inputPath = files[i % 2].getAbsolutePath();
            String outputPath = files[1 - i % 2].getAbsolutePath();

            LOGGER.info("Executing reduce binary " + binary + " on file " + inputPath);

            pb.command(binary,
                    "-i", inputPath,
                    "-o", outputPath);

            pb.start().waitFor();

            LOGGER.info("Reduce binary execution finished on file: " + inputPath);

            i++;
        }

        storage.putReduceFile(String.valueOf(destDirId), fileId, ClusterConfig.POD_NAME, files[binIds.size() % 2]);

        LOGGER.info("Reduce task finished for input file " + dataDir + "/" + fileId);
    }
}
