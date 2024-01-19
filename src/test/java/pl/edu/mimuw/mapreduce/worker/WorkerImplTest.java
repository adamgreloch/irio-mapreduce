package pl.edu.mimuw.mapreduce.worker;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.proto.common.Split;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.worker.DoMapRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class WorkerImplTest {
    private static Path tempDirPath;
    private static Server workerService;
    private static WorkerGrpc.WorkerBlockingStub blockingStub;
    private static Storage storage;

    static void whyCantIRemoveDirRecursivelyInJava(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                whyCantIRemoveDirRecursivelyInJava(file);
            }
        }
        directoryToBeDeleted.delete();
    }

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

    @Test
    public void workerImpl_correctlyDoesMap() throws Exception {

        var mapPath = getClass().getClassLoader().getResource("map");
        if (mapPath == null) throw new IOException("no test binary!");
        File mapBinary = new File(mapPath.toURI());

        var partitionPath = getClass().getClassLoader().getResource("partition");
        if (partitionPath == null) throw new IOException("no test binary!");
        File partitionBinary = new File(partitionPath.toURI());

        storage.putFile(Storage.BINARY_DIR, 0, mapBinary);
        storage.putFile(Storage.BINARY_DIR, 1, partitionBinary);

        var dataDirPath = Files.createDirectory(tempDirPath.resolve("0"));

        var mapInput1Str = "a a a a a b b b b";
        var mapInput1 = new BufferedWriter(new FileWriter(dataDirPath.resolve("1").toString()));
        mapInput1.write(mapInput1Str);
        mapInput1.close();

        var mapInput2Str = "a a b b b c c c";
        var mapInput2 = new BufferedWriter(new FileWriter(dataDirPath.resolve("2").toString()));
        mapInput2.write(mapInput2Str);
        mapInput2.close();

        var taskBinIds = List.of(0L, 1L);

        var task = Task.newBuilder()
                       .setTaskId(0)
                       .setTaskType(Task.TaskType.Map)
                       .setInputDirId("0")
                       .setDestDirId("1")
                       .addAllTaskBinIds(taskBinIds)
                       .build();

        var split = Split.newBuilder().setBeg(1).setEnd(2).build();

        var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();

        var response = blockingStub.doMap(doMapRequest);

        assertEquals(StatusCode.Ok, response.getStatusCode());

        var destDirDirPath = tempDirPath.resolve("1");
        var partitionOutput1 = new BufferedReader(new FileReader(destDirDirPath.resolve("1").toString()));
        var partitionOutput1Content = partitionOutput1.lines().collect(Collectors.joining(System.lineSeparator()));
        assertEquals("""
                a 1
                a 1
                a 1
                a 1
                a 1
                b 1
                b 1
                b 1
                b 1""", partitionOutput1Content);

        var partitionOutput2 = new BufferedReader(new FileReader(destDirDirPath.resolve("2").toString()));
        var partitionOutput2Content = partitionOutput2.lines().collect(Collectors.joining(System.lineSeparator()));
        assertEquals("""
                a 1
                a 1
                b 1
                b 1
                b 1
                c 1
                c 1
                c 1""", partitionOutput2Content);

        System.out.println(response);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        whyCantIRemoveDirRecursivelyInJava(tempDirPath.toFile());
        workerService.shutdownNow().awaitTermination();
    }

}
