package pl.edu.mimuw.mapreduce.worker;

import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class Processor implements AutoCloseable {
    private final long destinationId;
    private final File binary;
    private final String tempDir;
    private final Storage storage;

    public Processor(Storage storage, long binId, long destinationId) throws IOException {
        this.storage = storage;
        this.binary = storage.get_binary(binId);
        this.destinationId = destinationId;
        this.tempDir = Files.createTempDirectory("processor").toFile().getAbsolutePath();
    }

    public void process_file(FileRep fr) throws IOException, InterruptedException {
        var pb = new ProcessBuilder();
        pb.redirectInput(fr.file());
        var outputFile = new File(tempDir, String.valueOf(fr.id()));
        pb.redirectOutput(outputFile);
        pb.command(binary.getAbsolutePath());
        pb.start().waitFor();
        storage.put_file(destinationId, outputFile);
        Files.delete(outputFile.toPath().toAbsolutePath()); // are toAbsolutePath() transforms necessary?
    }

    @Override
    public void close() throws IOException {
        Files.delete(this.binary.toPath().toAbsolutePath());
    }
}
