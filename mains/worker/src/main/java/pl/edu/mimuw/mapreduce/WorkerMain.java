package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.config.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.worker.WorkerImpl;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerMain {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.worker");

    public static void main(String[] args) throws IOException, InterruptedException {
        var port = ClusterConfig.WORKERS_PORT;
        logger.log(Level.INFO, "Worker starting on port " + port);
        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        Utils.start_service(new WorkerImpl(storage), port);
    }
}
