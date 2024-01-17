package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.config.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;

import java.io.IOException;
import java.util.logging.Level;

public class TaskManagerMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from TaskManager!");

        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        Utils.start_service(new TaskManagerImpl(storage), ClusterConfig.TASK_MANAGERS_URI);
    }
}
