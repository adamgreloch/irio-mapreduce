package pl.edu.mimuw.mapreduce;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;

public class Utils {
    public static void start_service(io.grpc.BindableService service, int port) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(port)
                .addService(service)
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
}
