package pl.edu.mimuw.mapreduce.worker;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.worker.Request;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.IOException;

public class Worker {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new WorkerImpl(), 50042);
    }

    static class WorkerImpl extends WorkerGrpc.WorkerImplBase {
        @Override
        public void doWork(Request request, StreamObserver<Response> responseObserver) {
            throw new RuntimeException("todo");
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
