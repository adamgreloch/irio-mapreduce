package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.config.ClusterConfig;
import pl.edu.mimuw.mapreduce.master.MasterImpl;

import java.io.IOException;
import java.util.logging.Level;

public class MasterMain {

    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from Master!");

        Utils.start_service(new MasterImpl(), ClusterConfig.MASTERS_URI);
    }
}
