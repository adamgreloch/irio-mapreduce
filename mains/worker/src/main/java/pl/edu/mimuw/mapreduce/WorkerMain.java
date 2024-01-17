package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.config.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.worker.WorkerImpl;

import java.io.IOException;
import java.util.logging.Level;

public class WorkerMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from Worker!");

        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        Utils.start_service(new WorkerImpl(storage), ClusterConfig.WORKERS_URI);
    }
}
