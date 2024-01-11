package pl.edu.mimuw.mapreduce;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerMain {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.worker");

    public static void main(String[] args) throws IOException, InterruptedException {
        var port = 5042;
        logger.log(Level.INFO, "Worker starting on port " + port);
        // TODO: wait for working storage impl
        //Storage storage = new LocalStorage();
        //Utils.start_service(new WorkerImpl(storage), port);
    }
}
