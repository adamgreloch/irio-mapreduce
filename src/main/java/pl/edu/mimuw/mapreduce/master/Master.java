package pl.edu.mimuw.mapreduce.master;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.Batch;
import pl.edu.mimuw.mapreduce.healthcheck.Ping;
import pl.edu.mimuw.mapreduce.healthcheck.PingResponse;

import java.io.IOException;

public class Master {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new MasterImpl(), 50040);
    }

    static class MasterImpl extends MasterGrpc.MasterImplBase {
        @Override
        public void submitBatch(Batch request, StreamObserver<BatchDone> responseObserver) {
            throw new RuntimeException("todo");
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
