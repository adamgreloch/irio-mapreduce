package pl.edu.mimuw.mapreduce.master;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.batchmanager.BatchManagerImpl;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import static org.junit.Assert.assertEquals;

public class MasterImplTest {
    public class BatchManagerThread extends Thread {

        public void run() {
            try {
                BatchManagerImpl.start();
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    public class TaskManagerThread extends Thread {

        public void run() {
            try {
                TaskManagerImpl.start();
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Test
    public void masterImpl_shouldDoHealthCheck() throws Exception {
        // Run a TaskManager instance
        Thread thread = new BatchManagerThread();
        thread.start();

        Thread taskManagerThread = new TaskManagerThread();
        taskManagerThread.start();

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        var x = grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(new MasterImpl()).build().start());

        MasterGrpc.MasterBlockingStub blockingStub = MasterGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        PingResponse response =
                blockingStub.healthCheck(pl.edu.mimuw.proto.healthcheck.Ping.newBuilder().build());

        assertEquals(HealthStatusCode.Error, response.getStatusCode());
        assertEquals(MissingConnectionWithLayer.Worker_VALUE, response.getMissingLayerValue());

        x.shutdownNow();
    }

}
