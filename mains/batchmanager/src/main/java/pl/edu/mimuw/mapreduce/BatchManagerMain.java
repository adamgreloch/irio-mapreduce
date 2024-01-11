package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.batchmanager.BatchManagerImpl;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BatchManagerMain {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.batchmanager");

    public static void main(String[] args) throws IOException, InterruptedException {
        var port = 5043;
        logger.log(Level.INFO, "Batch manager starting on port " + port);
        Utils.start_service(new BatchManagerImpl(), port);
    }
}
