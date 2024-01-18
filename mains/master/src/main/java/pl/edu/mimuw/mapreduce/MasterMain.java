package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.master.MasterImpl;

import java.io.IOException;

public class MasterMain {

    public static void main(String[] args) throws IOException, InterruptedException {
        MasterImpl.start();
    }
}
