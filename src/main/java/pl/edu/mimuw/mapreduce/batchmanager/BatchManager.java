package pl.edu.mimuw.mapreduce.batchmanager;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.batchmanager.BatchManagerGrpc;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchManager {

    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new BatchManagerImpl(), 50043);
    }

    static class BatchManagerImpl extends BatchManagerGrpc.BatchManagerImplBase {
        private AtomicInteger taskNr = new AtomicInteger(1);
        private Map<Integer, Batch> currentBatches = new ConcurrentHashMap<>();
        private Map<Integer, Integer> doneTasks = new ConcurrentHashMap<>();
        private Map<Integer, Boolean> finishedMapping = new ConcurrentHashMap<>();

        @Override
        public void doBatch(Batch request, StreamObserver<Response> responseObserver) {
            throw new RuntimeException("todo");
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
