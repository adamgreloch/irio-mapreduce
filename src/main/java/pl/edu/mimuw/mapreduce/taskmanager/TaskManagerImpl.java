package pl.edu.mimuw.mapreduce.taskmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.common.HealthCheckable;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.proto.common.*;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.taskmanager.TaskManagerGrpc;
import pl.edu.mimuw.proto.worker.DoMapRequest;
import pl.edu.mimuw.proto.worker.DoReduceRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase implements HealthCheckable {
    private final Storage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ManagedChannel workerChannel;
    private final HealthStatusManager health;

    public TaskManagerImpl(Storage storage, HealthStatusManager health, String workersUri) {
        this.storage = storage;
        this.health = health;
        Utils.LOGGER.log(Level.INFO, "Worker service URI set to: " + workersUri);
        this.workerChannel = Utils.createCustomClientChannelBuilder(workersUri).executor(pool).build();
    }

    public static void start() throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from TaskManager!");

        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        HealthStatusManager health = new HealthStatusManager();

        Utils.start_server(new TaskManagerImpl(storage, health, ClusterConfig.WORKERS_URI), health,
                ClusterConfig.TASK_MANAGERS_URI).awaitTermination();
    }

    private FutureCallback<Response> createWorkerResponseCallback(StreamObserver<Response> responseObserver,
                                                                  CountDownLatch operationsDoneLatch) {
        return new FutureCallback<>() {
            @Override
            public void onSuccess(Response result) {
                if (result.getStatusCode() == StatusCode.Err) {
                    throw new RuntimeException("todo: handle worker failure");
                }
                operationsDoneLatch.countDown();
            }

            /** Propagate error message. */
            @Override
            public void onFailure(Throwable t) {
                Utils.respondWithThrowable(t, responseObserver);
            }
        };
    }

    @Override
    public void doBatch(Batch batch, StreamObserver<Response> responseObserver) {
        pool.execute(new Handler(batch, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        Utils.LOGGER.log(Level.FINE, "Received health check request");
        var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

        var listenableFuture = workerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver,
                MissingConnectionWithLayer.Worker), pool);

    }

    @Override
    public PingResponse internalHealthcheck() {
        Utils.LOGGER.log(Level.SEVERE, "healthchecking not implemented");
        return PingResponse.getDefaultInstance();
    }

    class Handler implements Runnable {
        private final Batch batch;
        private final StreamObserver<Response> responseObserver;
        private CountDownLatch phaseDoneLatch;
        private final int splitsNum;
        private final AtomicInteger nextTaskId;
        private final Integer reduceTaskCount;
        private final List<String> workersDestDirIds; // List of directories that belong do batch.
        private final String finalDestDirId;

        Handler(Batch batch, StreamObserver<Response> responseObserver) {
            this.batch = batch;
            this.responseObserver = responseObserver;
            this.splitsNum = 10;
            this.phaseDoneLatch = new CountDownLatch(splitsNum);
            this.nextTaskId = new AtomicInteger(0);
            this.workersDestDirIds = new ArrayList<>();
            this.reduceTaskCount = batch.getReduceBinIdsCount();
            this.finalDestDirId = batch.getFinalDestDirId();
        }

        private Task createMapTask() {
            return Task.newBuilder()
                    .setTaskId(nextTaskId.getAndIncrement())
                    .setTaskType(Task.TaskType.Map)
                    .setInputDirId(batch.getInputId())
                    .setDestDirId(UUID.randomUUID().toString())
                    .addAllTaskBinIds(batch.getMapBinIdsList())
                    .addAllTaskBinIds(List.of((batch.getPartitionBinId())))
                    .build();
        }

        private Task createReduceTask(String inputDir) {
            return Task.newBuilder()
                    .setTaskId(nextTaskId.getAndIncrement())
                    .setTaskType(Task.TaskType.Reduce)
                    .setInputDirId(inputDir)
                    .setDestDirId(batch.getFinalDestDirId())
                    .addAllTaskBinIds(batch.getMapBinIdsList())
                    .addAllTaskBinIds(List.of((batch.getPartitionBinId())))
                    .build();
        }

        @Override
        public void run() {

            // Mapping phase
            List<Split> splits = storage.getSplitsForDir(batch.getInputId(), splitsNum);

            for (Split split : splits) {
                var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                Task task = createMapTask();
                workersDestDirIds.add(task.getDestDirId());

                var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();
                ListenableFuture<Response> listenableFuture = workerFutureStub.doMap(doMapRequest);
                Futures.addCallback(listenableFuture,
                        createWorkerResponseCallback(responseObserver, phaseDoneLatch),
                        pool);
            }

            // Concatenation phase

            try {
                phaseDoneLatch.await();
            } catch (InterruptedException e) {
                Utils.respondWithThrowable(e, responseObserver);
            }

            List<Integer> fileIds = new ArrayList<>();

            assert !workersDestDirIds.isEmpty();

            for (int i = 0; i < reduceTaskCount; i++) {
                File mergedFile = new File(storage.getDirPath(finalDestDirId).resolve(String.valueOf(i)).toUri());

                for (var workersDestDirId : workersDestDirIds) {
                    try (var outputStream = new BufferedOutputStream(new FileOutputStream(mergedFile, true))) {
                        File file = storage.getFile(workersDestDirId, i).file();
                        try (var inputStream = new BufferedInputStream(new FileInputStream(file))) {
                            IOUtils.copy(inputStream, outputStream);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (IOException e) {
                        Utils.respondWithThrowable(e, responseObserver);
                    }
                }

                storage.putFile(finalDestDirId, i, mergedFile);
                fileIds.add(i);
            }

            // Reduce phase

            phaseDoneLatch = new CountDownLatch(splitsNum);

            for (var fileId : fileIds) {
                var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                Task task = createReduceTask(finalDestDirId);

                var doReduceRequest = DoReduceRequest.newBuilder().setTask(task).setFileId(fileId).build();
                ListenableFuture<Response> listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, phaseDoneLatch), pool);
            }

            try {
                phaseDoneLatch.await();
            } catch (InterruptedException e) {
                Utils.respondWithThrowable(e, responseObserver);
            }

            Utils.respondWithSuccess(responseObserver);
        }
    }
}