package pl.edu.mimuw.mapreduce;

import com.google.common.util.concurrent.FutureCallback;
import io.grpc.BindableService;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.common.HealthCheckable;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.PingResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
    public static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    static class HealthCheckThread<S extends BindableService & HealthCheckable> extends Thread {
        private final S service;
        private final HealthStatusManager health;

        HealthCheckThread(S service, HealthStatusManager health) {
            this.service = service;
            this.health = health;
        }

        public void run() {
            while (true) {
                Utils.LOGGER.log(Level.FINE, "Performing periodic healthcheck...");

                var response = service.internalHealthcheck();
                if (response.getStatusCode() == HealthStatusCode.Healthy) {
                    Utils.LOGGER.log(Level.FINE, "Service is healthy");
                    health.setStatus("", ServingStatus.SERVING);
                } else {
                    Utils.LOGGER.log(Level.WARNING, "Service is unhealthy! " + response);
                    health.setStatus("", ServingStatus.NOT_SERVING);
                }
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    Utils.LOGGER.log(Level.SEVERE, "Health checking interrupted: " + e.getMessage());
                }
            }
        }
    }

    public static<S extends BindableService & HealthCheckable>  Server start_server(S service, HealthStatusManager health, String target) throws IOException {
        int port = Integer.parseInt(target.split(":")[1]);
        Server server =
                ServerBuilder.forPort(port)
                        .addService(service)
                        .addService(ProtoReflectionService.newInstance())
                        .addService(health.getHealthService())
                        .build();

        server.start();

        // https://grpc.io/docs/guides/health-checking/
        health.setStatus("", ServingStatus.SERVING);

        Utils.LOGGER.log(Level.INFO,
                "Started service " + service.getClass().getSimpleName() + " on port " + server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Utils.LOGGER.log(Level.INFO, "Received shutdown request");
            server.shutdown();
            Utils.LOGGER.log(Level.INFO, "Successfully stopped the server");
        }));

        (new HealthCheckThread<>(service, health)).start();

        return server;
    }

    private static Map<String, Object> generateHealthConfig(String serviceName) {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> serviceMap = new HashMap<>();

        config.put("healthCheckConfig", serviceMap);
        serviceMap.put("serviceName", serviceName);
        return config;
    }

    public static ManagedChannelBuilder<?> createCustomClientChannelBuilder(String target) {
        return ManagedChannelBuilder.forTarget(target)
                .defaultLoadBalancingPolicy("round_robin")
                .defaultServiceConfig(generateHealthConfig(""))
                // TODO probably worth enabling
                // .enableRetry()
                // .keepAliveTime(10, TimeUnit.SECONDS)
                .usePlaintext();
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
                PingResponse pingResponse = PingResponse.newBuilder()
                        .setStatusCode(HealthStatusCode.Error)
                        .setMissingLayer(connectingTo)
                        .build();
                responseObserver.onNext(pingResponse);
                responseObserver.onCompleted();
            }
        };
    }

    public static void respondWithThrowable(Throwable e, StreamObserver<Response> responseObserver) {
        Utils.LOGGER.log(Level.SEVERE, "RPC request failed with: " + e.getMessage());
        var response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(e.getMessage()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void respondWithSuccess(StreamObserver<Response> responseObserver) {
        Utils.LOGGER.log(Level.FINE, "RPC request succeeded");
        var response = Response.newBuilder().setStatusCode(StatusCode.Ok).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
