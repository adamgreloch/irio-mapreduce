package pl.edu.mimuw.mapreduce;

import com.google.common.util.concurrent.FutureCallback;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.PingResponse;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
    public static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    public static void start_service(io.grpc.BindableService service, String target) throws IOException,
            InterruptedException {

        int port = Integer.parseInt(target.split(":")[1]);
        Server server =
                ServerBuilder.forPort(port).addService(service).addService(ProtoReflectionService.newInstance()).build();

        server.start();

        Utils.LOGGER.log(Level.INFO, "Started service " + service.getClass().getSimpleName() + " on " + target);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Utils.LOGGER.log(Level.INFO, "Received shutdown request");
            server.shutdown();
            Utils.LOGGER.log(Level.INFO, "Successfully stopped the server");
        }));

        server.awaitTermination();
    }

    public static ManagedChannelBuilder<?> createCustomManagedChannelBuilder(String target) {
        return ManagedChannelBuilder.forTarget(target).defaultLoadBalancingPolicy("round_robin");
    }

    public static FutureCallback<PingResponse> createHealthCheckResponse(StreamObserver<PingResponse> responseObserver, MissingConnectionWithLayer connectingTo) {
        return new FutureCallback<PingResponse>() {
            @Override
            public void onSuccess(PingResponse result) {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                PingResponse pingResponse =
                        PingResponse.newBuilder().setStatusCode(HealthStatusCode.Error).setMissingLayer(MissingConnectionWithLayer.BatchManager).build();
                responseObserver.onNext(pingResponse);
                responseObserver.onCompleted();
            }
        };
    }
}
