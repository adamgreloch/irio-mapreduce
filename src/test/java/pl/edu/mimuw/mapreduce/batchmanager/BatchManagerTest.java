package pl.edu.mimuw.mapreduce.batchmanager;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManager;
import pl.edu.mimuw.proto.batchmanager.BatchManagerGrpc;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class BatchManagerTest {

    public class TaskManagerThread extends Thread {

        /** Run a TaskManager instance on port 2137. */
        public void run(){
            try {
                TaskManager.start(2137);
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Test
    public void batchManagerImpl_shouldDoBatch() throws Exception {
        // Run a TaskManager instance
        Thread thread = new TaskManagerThread();
        thread.start();

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(new BatchManager.BatchManagerImpl()).build().start());

        BatchManagerGrpc.BatchManagerBlockingStub blockingStub = BatchManagerGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
        var maps = List.of(1L, 2L, 3L);
        var reduces = List.of(3L, 4L);

        Response response =
                blockingStub.doBatch(Batch.newBuilder()
                        .setInputId(1)
                        .addAllMapBinIds(maps)
                        .addAllReduceBinIds(reduces)
                        .build());

        assertEquals(StatusCode.Ok, response.getStatusCode());
    }
}
