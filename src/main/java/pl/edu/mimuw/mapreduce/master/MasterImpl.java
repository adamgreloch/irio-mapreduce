package pl.edu.mimuw.mapreduce.master;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.common.HealthCheckable;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;
import pl.edu.mimuw.proto.taskmanager.TaskManagerGrpc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

public class MasterImpl extends MasterGrpc.MasterImplBase implements HealthCheckable {
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ManagedChannel taskManagerChannel;
    private final HealthStatusManager health;

    public MasterImpl(HealthStatusManager health, String taskManagersUri) {
        this.health = health;
        Utils.LOGGER.log(Level.INFO, "Task managers service URI set to: " + taskManagersUri);
        this.taskManagerChannel = Utils.createCustomClientChannelBuilder(taskManagersUri).executor(pool).build();
    }

    public static void start() throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from Master!");

        HealthStatusManager health = new HealthStatusManager();

        Utils.start_server(new MasterImpl(health, ClusterConfig.TASK_MANAGERS_URI), health,
                ClusterConfig.MASTERS_URI).awaitTermination();
    }

    private FutureCallback<Response> createCallback(StreamObserver<Response> responseObserver) {
        return new FutureCallback<Response>() {
            @Override
            public void onSuccess(Response result) {
                Utils.respondWithSuccess(responseObserver);
            }

            @Override
            public void onFailure(Throwable t) {
                Utils.respondWithThrowable(t, responseObserver);
            }
        };
    }

    @Override
    public void submitBatch(Batch request, StreamObserver<Response> responseObserver) {
        var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(taskManagerChannel);

        ListenableFuture<Response> listenableFuture = taskManagerFutureStub.doBatch(request);
        Futures.addCallback(listenableFuture, createCallback(responseObserver), pool);
    }

    @Override
    public PingResponse internalHealthcheck() {
        // TODO this probably can be done better with a listener plugged to the healthCheck call, but
        //  for now it suffices

        Utils.LOGGER.log(Level.FINE, "Performing healthcheck...");
        var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(taskManagerChannel);

        ListenableFuture<PingResponse> listenableFuture = taskManagerFutureStub.healthCheck(Ping.getDefaultInstance());
        try {
            return listenableFuture.get();
        } catch (ExecutionException e) {
            Utils.LOGGER.log(Level.SEVERE, "Lower layer unavailable: " + e.getMessage());
            return PingResponse.newBuilder().setStatusCode(HealthStatusCode.Error).setMissingLayer(MissingConnectionWithLayer.TaskManager).build();
        } catch (Exception e) {
            Utils.LOGGER.log(Level.SEVERE, "Unhandled exception when health checking: " + e);
            return null;
        }
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        Utils.LOGGER.log(Level.FINE, "Received health check request");

        var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(taskManagerChannel);

        ListenableFuture<PingResponse> listenableFuture =
                taskManagerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver,
                MissingConnectionWithLayer.TaskManager), pool);
    }
}
