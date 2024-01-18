package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.worker.WorkerImpl;

import java.io.IOException;

public class WorkerMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        WorkerImpl.start();
    }
}
