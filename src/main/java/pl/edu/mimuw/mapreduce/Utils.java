package pl.edu.mimuw.mapreduce;

import com.google.common.util.concurrent.FutureCallback;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.PingResponse;

import java.io.IOException;

public class Utils {
    public static void start_service(io.grpc.BindableService service, int port) throws IOException,
            InterruptedException {
        Server server =
                ServerBuilder.forPort(port).addService(service).addService(ProtoReflectionService.newInstance()) //
                // reflection
                .build();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received Shutdown Request");
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));

        server.awaitTermination();
    }

    public static int log2(int bits)
    {
        int log = 0;
        if ((bits & 0xffff0000) != 0) {
            bits >>>= 16;
            log = 16;
        }
        if (bits >= 256) {
            bits >>>= 8;
            log += 8;
        }
        if (bits >= 16) {
            bits >>>= 4;
            log += 4;
        }
        if (bits >= 4) {
            bits >>>= 2;
            log += 2;
        }
        return log + (bits >>> 1);
    }

    public static FutureCallback<PingResponse> createHealthCheckResponse(StreamObserver<PingResponse> responseObserver,
                                                                  MissingConnectionWithLayer connectingTo) {
        return new FutureCallback<PingResponse>() {
            @Override
            public void onSuccess(PingResponse result) {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                PingResponse pingResponse = PingResponse.newBuilder()
                        .setStatusCode(HealthStatusCode.Error)
                        .setMissingLayer(MissingConnectionWithLayer.BatchManager)
                        .build();
                responseObserver.onNext(pingResponse);
                responseObserver.onCompleted();
            }
        };
    }
}
