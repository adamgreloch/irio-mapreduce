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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.common.HealthCheckable;
import pl.edu.mimuw.mapreduce.serverbreaker.ServerBreakerImpl;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.processbreaker.Action;
import pl.edu.mimuw.proto.processbreaker.Payload;
import pl.edu.mimuw.proto.worker.DoMapRequest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Utils {
    public static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    static class HealthCheckThread<S extends BindableService & HealthCheckable> extends Thread {
        private final S service;
        private final HealthStatusManager health;

        HealthCheckThread(S service, HealthStatusManager health) {
            this.service = service;
            this.health = health;
        }

        public void run() {
            while (true) {
                Utils.LOGGER.trace("Performing periodic healthcheck...");

                var response = service.internalHealthcheck();
                if (response.getStatusCode() == HealthStatusCode.Healthy) {
                    Utils.LOGGER.trace("Service is healthy");
                    health.setStatus("", ServingStatus.SERVING);
                } else {
                    Utils.LOGGER.warn("Service is unhealthy! " + response);
                    health.setStatus("", ServingStatus.NOT_SERVING);
                }
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    Utils.LOGGER.error("Health checking interrupted: " + e.getMessage());
                }
            }
        }
    }

    public static void removeDirRecursively(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                removeDirRecursively(file);
            }
        }
        directoryToBeDeleted.delete();
    }


    public static <S extends BindableService & HealthCheckable> Server start_server(S service, HealthStatusManager health, String target) throws IOException {
        int port = Integer.parseInt(target.split(":")[1]);
        Server server =
                ServerBuilder.forPort(port)
                        .addService(service)
                        .addService(ProtoReflectionService.newInstance())
                        .addService(health.getHealthService())
                        .addService(ServerBreakerImpl.getInstance())
                        .build();

        server.start();

        // https://grpc.io/docs/guides/health-checking/
        health.setStatus("", ServingStatus.SERVING);

        Utils.LOGGER.info("Started service " + service.getClass().getSimpleName() + " on port " + server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Utils.LOGGER.info("Received shutdown request");
            server.shutdown();
            Utils.LOGGER.info("Successfully stopped the server");
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
        Utils.LOGGER.trace("RPC request failed with: " + e.getMessage());
        var response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(e.getMessage()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void respondWithSuccess(StreamObserver<Response> responseObserver) {
        Utils.LOGGER.trace("RPC request succeeded");
        var response = Response.newBuilder().setStatusCode(StatusCode.Ok).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void respondToHealthcheck(StreamObserver<PingResponse> responseObserver, HealthStatusCode code) {
        PingResponse pingResponse = PingResponse.newBuilder().setStatusCode(code).build();
        responseObserver.onNext(pingResponse);
        responseObserver.onCompleted();
    }

    public static void handleServerBreakerAction(StreamObserver<Response> responseObserver) {
        Payload payload = ServerBreakerImpl.getInstance().getPayload();
        handlePayload(payload);
        if (payload.getAction() == Action.FAIL_ALWAYS) {
            Utils.respondWithThrowable(new RuntimeException("Always failing flag is set."), responseObserver);
        }
    }

    //Returns true if response was made.
    public static boolean handleServerBreakerHealthCheckAction(StreamObserver<PingResponse> responseObserver) {
        Payload payload = ServerBreakerImpl.getInstance().getPayload();
        handlePayload(payload);
        if (payload.getAction() == Action.FAIL_ALWAYS) {
            Utils.respondToHealthcheck(responseObserver, HealthStatusCode.Error);
            return true;
        }
        return false;
    }

    // Returns true if we should fail internal healthcheck
    public static boolean handleServerBreakerInternalHealthCheckAction() {
        Payload payload = ServerBreakerImpl.getInstance().getPayload();
        handlePayload(payload);
        return payload.getAction() == Action.FAIL_ALWAYS;
    }

    private static void handlePayload(Payload payload) {
        switch (payload.getAction()) {
            case KILL -> System.exit(1);
            case HANG -> {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(payload.getParam());
                    } catch (InterruptedException e) {
                        System.exit(0);
                    }
                }
            }
            case TIMEOUT -> {
                try {
                    TimeUnit.SECONDS.sleep(payload.getParam());
                } catch (InterruptedException e) {
                    System.exit(0);
                }
            }
        }
    }

    public static DoMapRequest changeDestDirIdInTask(DoMapRequest request) {
        return DoMapRequest.newBuilder().mergeFrom(request).setTask(
                Task.newBuilder().mergeFrom(request.getTask()).setDestDirId(UUID.randomUUID().toString()).build()
        ).build();
    }
}
