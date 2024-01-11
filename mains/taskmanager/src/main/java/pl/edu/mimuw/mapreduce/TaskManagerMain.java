package pl.edu.mimuw.mapreduce;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TaskManagerMain {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.taskmanager");

    public static void main(String[] args) throws IOException, InterruptedException {
        var port = 5044;
        logger.log(Level.INFO, "Task manager starting on port " + port);
        //Utils.start_service(new TaskManagerImpl(), port);
    }
}
