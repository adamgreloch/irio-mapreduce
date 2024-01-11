package pl.edu.mimuw.mapreduce.master;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Master {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.master");

    public static void main(String[] args) throws IOException, InterruptedException {
        var port = 50040;
        logger.log(Level.INFO, "Master starting on port " + port);
        Utils.start_service(new MasterImpl(), port);
    }

    static class MasterImpl extends MasterGrpc.MasterImplBase {
        @Override
        public void submitBatch(Batch request, StreamObserver<Response> responseObserver) {
            throw new RuntimeException("todo");
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
