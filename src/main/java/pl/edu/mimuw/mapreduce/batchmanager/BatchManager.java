package pl.edu.mimuw.mapreduce.batchmanager;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.Batch;
import pl.edu.mimuw.mapreduce.healthcheck.Ping;
import pl.edu.mimuw.mapreduce.healthcheck.PingResponse;

import java.io.IOException;

public class BatchManager {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new BatchManagerImpl(), 50043);
    }

    static class BatchManagerImpl extends BatchManagerGrpc.BatchManagerImplBase {
        @Override
        public void doBatch(Batch request, StreamObserver<BatchManagerDone> responseObserver) {
            throw new RuntimeException("todo");
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
