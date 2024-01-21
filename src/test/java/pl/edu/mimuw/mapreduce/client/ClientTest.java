package pl.edu.mimuw.mapreduce.client;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.master.MasterImpl;
import pl.edu.mimuw.mapreduce.serverbreaker.ServerBreakerImpl;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;
import pl.edu.mimuw.mapreduce.worker.WorkerImpl;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;
import pl.edu.mimuw.proto.processbreaker.ServerBreakerGrpc;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ClientTest {
    private static Path tempDirPath;
    private static Server workerService;
    private static WorkerGrpc.WorkerBlockingStub blockingStub;
    private static Storage storage;
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

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

    String readOutputFromFile(Path dirPath, long fileId) throws FileNotFoundException {
        var buf = new BufferedReader(new FileReader(dirPath.resolve(String.valueOf(fileId)).toString()));
        return buf.lines().collect(Collectors.joining(System.lineSeparator()));
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
        writeInputFileWithId(dataDirPath, "1", "d bi ooooooo c");
        writeInputFileWithId(dataDirPath, "2", "d b beee c");
        writeInputFileWithId(dataDirPath, "3", "d b beee beee  aaaa c");
        writeInputFileWithId(dataDirPath, "4", "d affffffffff  ffc");
        writeInputFileWithId(dataDirPath, "5", "a  j c j c j c j c j cj c");
        writeInputFileWithId(dataDirPath, "6", "a beee c");
        writeInputFileWithId(dataDirPath, "7", "a bbeee beee beee beee  c");
        writeInputFileWithId(dataDirPath, "8", "a bbeee bee  e beee beee  c");
        writeInputFileWithId(dataDirPath, "9", "a bbzzzz zzzzzzzzz beee  c");
        writeInputFileWithId(dataDirPath, "10", "a bzzzz zzzzzzzzze beee  c");
        writeInputFileWithId(dataDirPath, "11", "a bzzzz zzz zzzzzze beee  c");
        writeInputFileWithId(dataDirPath, "12", "a bzzzzzzzz zzzzze beee  c");

        storage.createDir("1");

        masterServer.awaitTermination();
        workerServer.shutdownNow().awaitTermination();
        taskManagerServer.shutdownNow().awaitTermination();
        masterServer.shutdownNow().awaitTermination();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        Utils.removeDirRecursively(tempDirPath.toFile());
    }
}
