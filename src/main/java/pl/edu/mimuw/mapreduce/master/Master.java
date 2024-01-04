package pl.edu.mimuw.mapreduce.master;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.io.IOException;

public class Master {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new MasterImpl(), 50040);
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
