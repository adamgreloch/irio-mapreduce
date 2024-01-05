package pl.edu.mimuw.mapreduce.worker;

import pl.edu.mimuw.mapreduce.storage.local.FileRep;
import pl.edu.mimuw.mapreduce.storage.local.LocalStorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class Processor implements AutoCloseable {
    private final long destination_id;
    private final File binary;
    private final String tempdir;
    private final LocalStorage storage;

    public Processor(LocalStorage storage, long bin_id, long destination_id) throws IOException {
        this.storage = storage;
        this.binary = storage.get(bin_id).getFile();
        this.destination_id = destination_id;
        this.tempdir = Files.createTempDirectory("processor").toFile().getAbsolutePath();
    }

    public void process_file(FileRep fr) throws IOException, InterruptedException {
        var pb = new ProcessBuilder();
        pb.redirectInput(fr.getFile());
        var outputFile = new File(tempdir, String.valueOf(fr.getId()));
        pb.redirectOutput(outputFile);
        pb.command(binary.getAbsolutePath());
        pb.start().waitFor();
        storage.put(outputFile);
        Files.delete(outputFile.toPath().toAbsolutePath()); // are toAbsolutePath() transforms necessary?
    }

    @Override
    public void close() throws IOException {
        Files.delete(this.binary.toPath().toAbsolutePath());
    }
}
