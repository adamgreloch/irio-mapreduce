package pl.edu.mimuw.mapreduce;

import pl.edu.mimuw.mapreduce.master.MasterImpl;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MasterMain {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.master");

    public static void main(String[] args) throws IOException, InterruptedException {
        var port = 5040;
        logger.log(Level.INFO, "Master starting on port " + port);
        Utils.start_service(new MasterImpl(), port);
    }
}
