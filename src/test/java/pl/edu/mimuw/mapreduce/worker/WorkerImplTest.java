package pl.edu.mimuw.mapreduce.worker;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.proto.common.Split;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.worker.DoMapRequest;
import pl.edu.mimuw.proto.worker.DoReduceRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class WorkerImplTest {
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
    public void workerImpl_correctlyDoesATask() throws Exception {
        loadBinaryFromResource("map", 0);
        loadBinaryFromResource("partition", 1);
        loadBinaryFromResource("reduce", 2);

        storage.createDir("0");
        var dataDirPath = storage.getDirPath("0");

        writeInputFileWithId(dataDirPath, "0", "a b c");
        writeInputFileWithId(dataDirPath, "1", "a b c");

        var taskBinIds = List.of(0L, 1L);

        storage.createDir("1");

        var task = Task.newBuilder()
                       .setTaskId(0)
                       .setTaskType(Task.TaskType.Map)
                       .setInputDirId("0")
                       .setDestDirId("1")
                       .addAllTaskBinIds(taskBinIds)
                       .build();

        var split = Split.newBuilder().setBeg(0).setEnd(1).build();

        var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();

        var response = blockingStub.doMap(doMapRequest);

        assertEquals(StatusCode.Ok, response.getStatusCode());

        var destDirDirPath = tempDirPath.resolve("1");
        var output = readOutputFromFile(destDirDirPath, 0);

        assertEquals("""
                a 1
                b 1
                c 1
                a 1
                b 1
                c 1""", output);

        storage.createDir("2");

        task = Task.newBuilder()
                   .setTaskId(1)
                   .setTaskType(Task.TaskType.Reduce)
                   .setInputDirId("1")
                   .setDestDirId("2")
                   .addAllTaskBinIds(Collections.singleton(2L))
                   .build();

        var doReduceRequest = DoReduceRequest.newBuilder().setTask(task).setFileId(0).build();

        response = blockingStub.doReduce(doReduceRequest);
        System.out.println(response);

        storage.removeReduceDuplicates("2");

        destDirDirPath = tempDirPath.resolve("2");
        output = readOutputFromFile(destDirDirPath, 0);

        assertEquals("""
                a 2
                b 2
                c 2""", output);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        Utils.removeDirRecursively(tempDirPath.toFile());
        workerService.shutdownNow().awaitTermination();
        storage.close();
    }

}
