package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.batchmanager.BatchManagerImpl;
import pl.edu.mimuw.mapreduce.config.ClusterConfig;

import java.io.IOException;
import java.util.logging.Level;

public class BatchManagerMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from BatchManager!");

        Utils.start_service(new BatchManagerImpl(), ClusterConfig.BATCH_MANAGERS_URI);
    }
}
