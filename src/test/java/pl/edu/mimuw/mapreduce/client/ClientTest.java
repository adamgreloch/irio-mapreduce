package pl.edu.mimuw.mapreduce.client;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.master.MasterImpl;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;
import pl.edu.mimuw.mapreduce.worker.WorkerImpl;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ClientTest {
    private static Path tempDirPath;
    private static Server workerService;
    private static WorkerGrpc.WorkerBlockingStub blockingStub;
    private static Storage storage;
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    String loadBatchJsonFromResource(String resourceName) throws IOException, URISyntaxException {
        var path = getClass().getClassLoader().getResource(resourceName);
        if (path == null) throw new IOException("no test batch json!");
        return Path.of(path.toURI()).toString();
    }

    @BeforeAll
    public static void createTempDir() throws Exception {
        tempDirPath = Files.createTempDirectory("worker_test");
        storage = new DistrStorage(tempDirPath.toAbsolutePath().toString());

        HealthStatusManager health = new HealthStatusManager();
        String workerName = InProcessServerBuilder.generateName();
        workerService = grpcCleanup.register(InProcessServerBuilder.forName(workerName)
                .directExecutor()
                .addService(new WorkerImpl(storage, health))
                .build()
                .start());

        blockingStub = WorkerGrpc.newBlockingStub(grpcCleanup.register(InProcessChannelBuilder.forName(workerName)
                .directExecutor()
                .useTransportSecurity()
                .build()));

    }

    void writeInputFileWithId(Path dataDir, String id, String content) throws IOException {
        var buf = new BufferedWriter(new FileWriter(dataDir.resolve(id).toString()));
        buf.write(content);
        buf.close();
    }

    void loadBinaryFromResource(String resourceName, long binId) throws IOException, URISyntaxException {
        var path = getClass().getClassLoader().getResource(resourceName);
        if (path == null) throw new IOException("no test binary!");
        File binary = new File(path.toURI());
        storage.putFile(Storage.BINARY_DIR, binId, binary);
    }

    @Test
    public void client_wholeSystemWorkingTest() throws Exception {
        HealthStatusManager masterHealth = new HealthStatusManager();
        var masterServer = Utils.start_server(new MasterImpl(masterHealth, ClusterConfig.TASK_MANAGERS_URI),
                masterHealth, ClusterConfig.MASTERS_URI);

        HealthStatusManager taskManagerHealth = new HealthStatusManager();
        var taskManagerServer = Utils.start_server(new TaskManagerImpl(storage, taskManagerHealth,
                ClusterConfig.WORKERS_URI), taskManagerHealth, ClusterConfig.TASK_MANAGERS_URI);

        HealthStatusManager workerHealth = new HealthStatusManager();
        var workerServer = Utils.start_server(new WorkerImpl(storage, workerHealth), workerHealth,
                ClusterConfig.WORKERS_URI);

        loadBinaryFromResource("map", 0);
        loadBinaryFromResource("partition", 1);
        loadBinaryFromResource("reduce", 2);

        storage.createDir("0");
        var dataDirPath = storage.getDirPath("0");

        writeInputFileWithId(dataDirPath, "0", "a b c");
        writeInputFileWithId(dataDirPath, "1", "a b c");

        storage.createDir("1");

        var path = loadBatchJsonFromResource("client/batch-resource.json");
        if (path == null) throw new IOException("no json batch resource!");
        Client.main(new String[]{path});
        Thread.sleep(2000);

        var output = Utils.readOutputFromFile(tempDirPath.resolve("1"), 0);
        assertEquals("""
                a 2
                b 2
                c 2""", output);

        // nastepny task - zmienic binarke ze sleepem, zajebac workera i sprawdzic czy drugi worker zadziala


        masterServer.shutdownNow().awaitTermination();
        workerServer.shutdownNow().awaitTermination();
        taskManagerServer.shutdownNow().awaitTermination();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        Utils.removeDirRecursively(tempDirPath.toFile());
    }
}
