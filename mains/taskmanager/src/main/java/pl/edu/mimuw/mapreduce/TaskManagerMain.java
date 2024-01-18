package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;

import java.io.IOException;

public class TaskManagerMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        TaskManagerImpl.start();
    }
}
