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
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase implements HealthCheckable {
    private final Storage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(10);
    private final ManagedChannel workerChannel;
    private final HealthStatusManager health;
    private final Integer MAX_ATTEMPT = 3;
    private final Integer WORKER_TIMEOUT = 300; // Time after which task will be rerun in seconds.

    public TaskManagerImpl(Storage storage, HealthStatusManager health, String workersUri) {
        this.storage = storage;
        this.health = health;
        Utils.LOGGER.info("Worker service URI set to: " + workersUri);
        this.workerChannel = Utils.createCustomClientChannelBuilder(workersUri).executor(pool).build();
    }

    public static void start() throws IOException, InterruptedException {
        Utils.LOGGER.info("Hello from TaskManager!");

        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        HealthStatusManager health = new HealthStatusManager();

        Utils.start_server(new TaskManagerImpl(storage, health, ClusterConfig.WORKERS_URI), health,
                        ClusterConfig.TASK_MANAGERS_URI)
                .awaitTermination();
    }

    @Override
    public void doBatch(Batch batch, StreamObserver<Response> responseObserver) {
        Utils.handleServerBreakerAction(responseObserver);
        pool.execute(new BatchHandler(batch, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        if (Utils.handleServerBreakerHealthCheckAction(responseObserver)) {
            return;
        }
        Utils.LOGGER.trace("Received health check request");
        var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

        var listenableFuture = workerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver,
                MissingConnectionWithLayer.Worker), pool);

    }

    @Override
    public PingResponse internalHealthcheck() {
        if (Utils.handleServerBreakerInternalHealthCheckAction()) {
            return PingResponse.newBuilder().setStatusCode(HealthStatusCode.Error).build();
        }
        Utils.LOGGER.warn("healthchecking not implemented");
        return PingResponse.getDefaultInstance();
    }

    class BatchHandler implements Runnable {
        private final Batch batch;
        private final StreamObserver<Response> responseObserver;
        private CountDownLatch phaseDoneLatch;
        private final int splitsNum;
        private final AtomicInteger nextTaskId;
        private final Integer reduceTaskCount;
        private final List<String> workersDestDirIds; // List of directories that belong do batch.
        private final String finalDestDirId;
        private final Map<DoMapRequest, List<ListenableFuture<Response>>> runningMaps;
        private final Map<DoReduceRequest, List<ListenableFuture<Response>>> runningReduces;
        private final Map<DoMapRequest, Boolean> completedMaps;
        private final Map<DoReduceRequest, Boolean> completedReduces;

        BatchHandler(Batch batch, StreamObserver<Response> responseObserver) {
            this.batch = batch;
            this.responseObserver = responseObserver;
            this.splitsNum = 10;
            this.phaseDoneLatch = new CountDownLatch(splitsNum);
            this.nextTaskId = new AtomicInteger(0);
            this.workersDestDirIds = new ArrayList<>();
            this.reduceTaskCount = batch.getReduceBinIdsCount();
            this.finalDestDirId = batch.getFinalDestDirId();
            this.runningMaps = new ConcurrentHashMap<>();
            this.runningReduces = new ConcurrentHashMap<>();
            this.completedMaps = new ConcurrentHashMap<>();
            this.completedReduces = new ConcurrentHashMap<>();
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

                var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();
                ListenableFuture<Response> listenableFuture = workerFutureStub.doMap(doMapRequest);
                runningMaps.computeIfAbsent(doMapRequest, r -> new ArrayList<>()).add(listenableFuture);
                Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, phaseDoneLatch,
                        doMapRequest, 0, runningMaps, completedMaps), pool);
            }

            try {
                rescheduleIfStale(runningMaps, completedMaps);
            } catch (InterruptedException e) {
                Utils.respondWithThrowable(e, responseObserver);
                return;
            }

            // Concatenation phase

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
                runningReduces.computeIfAbsent(doReduceRequest, r -> new ArrayList<>()).add(listenableFuture);
                Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, phaseDoneLatch,
                        doReduceRequest, 0, runningReduces, completedReduces), pool);
            }

            try {
                rescheduleIfStale(runningReduces, completedReduces);
            } catch (InterruptedException e) {
                Utils.respondWithThrowable(e, responseObserver);
                return;
            }

            Utils.respondWithSuccess(responseObserver);
        }

        private <T> void rescheduleIfStale(Map<T, List<ListenableFuture<Response>>> runningRequests,
                                           Map<T, Boolean> completedRequests) throws InterruptedException {
            var latchStatus = false;
            while (!latchStatus) {
                latchStatus = phaseDoneLatch.await(WORKER_TIMEOUT, TimeUnit.SECONDS);
                if (!latchStatus) {
                    for (var request : runningRequests.keySet()) { // resend still running reduces
                        var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                        ListenableFuture<Response> listenableFuture;
                        if (request instanceof DoMapRequest) {
                            listenableFuture = workerFutureStub.doMap((DoMapRequest) request);
                        } else {
                            listenableFuture = workerFutureStub.doReduce((DoReduceRequest) request);

                        }
                        runningRequests.computeIfAbsent(request, r -> new ArrayList<>()).add(listenableFuture);
                        Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver,
                                phaseDoneLatch, request, 0, runningRequests, completedRequests), pool);
                    }
                }
            }
        }

        private <T> FutureCallback<Response> createWorkerResponseCallback(StreamObserver<Response> responseObserver,
                                                                          CountDownLatch operationsDoneLatch,
                                                                          T request, int attempt, Map<T,
                List<ListenableFuture<Response>>> runningRequests, Map<T, Boolean> completedRequests) {

            return new FutureCallback<>() {
                @Override
                public void onSuccess(Response result) {
                    if (result.getStatusCode() == StatusCode.Err) {
                        if (attempt > MAX_ATTEMPT) {
                            Utils.respondWithThrowable(new RuntimeException("Number of attempts for task was " +
                                    "exceeded"), responseObserver);
                            return;
                        }
                        reRunTask(responseObserver, operationsDoneLatch, attempt + 1);
                    } else if (completedRequests.putIfAbsent(request, true) != null) {
                        var futures = runningRequests.remove(request);
                        for (var future : futures) {
                            future.cancel(true);
                        }
                        if (request instanceof DoReduceRequest doReduceRequest)
                            workersDestDirIds.add(doReduceRequest.getTask().getDestDirId());
                        else if (request instanceof DoMapRequest doMapRequest)
                            workersDestDirIds.add(doMapRequest.getTask().getDestDirId());
                        else
                            throw new AssertionError("Object is neither an instance of DoMapRequest or " +
                                    "DoReduceRequest");
                        operationsDoneLatch.countDown();
                    } // else: some other worker's response has already been sent for that request, ignore
                }

                /** Propagate error message. */
                @Override
                public void onFailure(Throwable t) {
                    if (attempt > MAX_ATTEMPT) {
                        Utils.respondWithThrowable(t, responseObserver);
                    } else reRunTask(responseObserver, operationsDoneLatch, attempt + 1);
                }

                private void reRunTask(StreamObserver<Response> responseObserver, CountDownLatch operationsDoneLatch,
                                       int attempt) {
                    var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);
                    ListenableFuture<Response> listenableFuture;

                    if (request instanceof DoMapRequest doMapRequest) {
                        listenableFuture = workerFutureStub.doMap(doMapRequest);
                    } else if (request instanceof DoReduceRequest doReduceRequest) {
                        listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                    } else
                        throw new AssertionError("Object is neither an instance of DoMapRequest or " +
                                "DoReduceRequest");

                    Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver,
                            operationsDoneLatch, request, attempt + 1, runningRequests, completedRequests), pool);
                }
            };
        }

    }
}