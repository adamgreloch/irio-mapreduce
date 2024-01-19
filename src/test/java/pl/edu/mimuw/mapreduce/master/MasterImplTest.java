package pl.edu.mimuw.mapreduce.master;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;
import pl.edu.mimuw.mapreduce.worker.WorkerImpl;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class MasterImplTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Test
    public void masterImpl_correctlyHealthChecksLowerLayers() throws Exception {
        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);

        HealthStatusManager taskManagerHealth = new HealthStatusManager();
        var taskManagerServer = Utils.start_server(new TaskManagerImpl(storage, taskManagerHealth,
                ClusterConfig.WORKERS_URI), taskManagerHealth, ClusterConfig.TASK_MANAGERS_URI);

        HealthStatusManager workerHealth = new HealthStatusManager();
        var workerServer = Utils.start_server(new WorkerImpl(storage, workerHealth), workerHealth,
                ClusterConfig.WORKERS_URI);

        // Generate a unique in-process server name.
        String masterName = InProcessServerBuilder.generateName();

        HealthStatusManager masterHealth = new HealthStatusManager();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        var masterService =
                grpcCleanup.register(InProcessServerBuilder.forName(masterName).directExecutor().addService(new MasterImpl(masterHealth, ClusterConfig.TASK_MANAGERS_URI)).build().start());

        MasterGrpc.MasterBlockingStub blockingStub = MasterGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(masterName).directExecutor().useTransportSecurity().build()));

        TimeUnit.SECONDS.sleep(1);

        PingResponse response = blockingStub.healthCheck(Ping.newBuilder().build());

        // Everyone is alive here
        assertEquals(HealthStatusCode.Healthy, response.getStatusCode());

        workerServer.shutdownNow().awaitTermination();

        response = blockingStub.healthCheck(Ping.newBuilder().build());

        // Worker should be unavailable
        assertEquals(HealthStatusCode.Error, response.getStatusCode());
        assertEquals(MissingConnectionWithLayer.Worker, response.getMissingLayer());

        taskManagerServer.shutdownNow().awaitTermination();

        response = blockingStub.healthCheck(Ping.newBuilder().build());

        // Task manager should be unavailable
        assertEquals(HealthStatusCode.Error, response.getStatusCode());
        assertEquals(MissingConnectionWithLayer.TaskManager, response.getMissingLayer());

        masterService.shutdownNow().awaitTermination();
    }

    @Test
    public void masterImpl_rediscoversPreviouslyInaccessibleService() throws Exception {
        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);

        HealthStatusManager masterHealth = new HealthStatusManager();
        var masterImpl = new MasterImpl(masterHealth, ClusterConfig.TASK_MANAGERS_URI);
        var masterServer = Utils.start_server(masterImpl, masterHealth, ClusterConfig.MASTERS_URI);

        var response = masterImpl.internalHealthcheck();

        assertEquals(HealthStatusCode.Error, response.getStatusCode());
        assertEquals(MissingConnectionWithLayer.TaskManager, response.getMissingLayer());

        HealthStatusManager taskManagerHealth = new HealthStatusManager();
        var taskManagerServer = Utils.start_server(new TaskManagerImpl(storage, taskManagerHealth,
                ClusterConfig.WORKERS_URI), taskManagerHealth, ClusterConfig.TASK_MANAGERS_URI);

        TimeUnit.SECONDS.sleep(5);

        response = masterImpl.internalHealthcheck();

        assertEquals(HealthStatusCode.Error, response.getStatusCode());
        assertEquals(MissingConnectionWithLayer.Worker, response.getMissingLayer());

        taskManagerServer.shutdownNow().awaitTermination();
        masterServer.shutdownNow().awaitTermination();
    }

}
