package pl.edu.mimuw.mapreduce.worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.healthcheck.Ping;
import pl.edu.mimuw.mapreduce.healthcheck.PingResponse;

import java.io.IOException;

public class Worker {
    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(50042)
                .addService(new WorkerImpl())
                .addService(ProtoReflectionService.newInstance()) // reflection
                .build();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received Shutdown Request");
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));

        server.awaitTermination();
    }

    static class WorkerImpl extends WorkerGrpc.WorkerImplBase {
        @Override
        public void doTask(Request request, StreamObserver<Response> responseObserver) {
            // TODO
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            // TODO
        }
    }
}
