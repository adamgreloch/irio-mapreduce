package pl.edu.mimuw.mapreduce.master;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.util.logging.Logger;

public class MasterImpl extends MasterGrpc.MasterImplBase {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.master");

    @Override
    public void submitBatch(Batch request, StreamObserver<Response> responseObserver) {
        throw new RuntimeException("todo");
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        throw new RuntimeException("todo");
    }
}
