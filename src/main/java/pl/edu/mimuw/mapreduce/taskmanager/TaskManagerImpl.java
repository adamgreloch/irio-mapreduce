package pl.edu.mimuw.mapreduce.taskmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase implements HealthCheckable {
    public static final Logger LOGGER = LoggerFactory.getLogger(TaskManagerImpl.class);
    private final Storage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(10);
    private final ManagedChannel workerChannel;
    private final Integer MAX_ATTEMPT = 3;
    private final Integer WORKER_TIMEOUT = 30; // Time after which task will be rerun in seconds.

    public TaskManagerImpl(Storage storage, HealthStatusManager health, String workersUri) {
        this.storage = storage;
        LOGGER.info("Worker service URI set to: " + workersUri);
        this.workerChannel = Utils.createCustomClientChannelBuilder(workersUri).executor(pool).build();
    }

    public static void start() throws IOException, InterruptedException {
        LOGGER.info("Hello from TaskManager!");

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
        LOGGER.trace("Received health check request");
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
        return PingResponse.getDefaultInstance();
    }

    class BatchHandler implements Runnable {
        private final Batch batch;
        private final StreamObserver<Response> responseObserver;
        private CountDownLatch phaseDoneLatch;
        private final int splitCount;
        private final AtomicInteger nextTaskId = new AtomicInteger(0);
        private final List<String> workersDestDirIds = new ArrayList<>(); // List of directories that belong do batch.
        private final String concatDirId;
        private final String reduceDestDirId; // Temporary directory for reduce results before deduplication

        // Each Key in this map is a Task.ID
        private final Map<Long, List<ListenableFuture<Response>>> runningMaps = new ConcurrentHashMap<>();
        private final Map<Long, List<ListenableFuture<Response>>> runningReduces = new ConcurrentHashMap<>();

        // DO NOT USE basicMap for getting destDirId use workersDestDirIds instead;
        private final Map<Long, DoMapRequest> basicMap = new ConcurrentHashMap<>(); // First instance of request for
        // provided Id from which we should mutate our request if we must redo it.
        private final Map<Long, DoReduceRequest> basicReduce = new ConcurrentHashMap<>(); // First instance of
        // request for provided Id from which we should mutate our request if we must redo it.
        private final Map<Long, Boolean> completedMaps = new ConcurrentHashMap<>();
        private final Map<Long, Boolean> completedReduces = new ConcurrentHashMap<>();

        BatchHandler(Batch batch, StreamObserver<Response> responseObserver) {
            this.batch = batch;
            this.responseObserver = responseObserver;
            this.splitCount = batch.getSplitCount();
            this.phaseDoneLatch = new CountDownLatch(splitCount);

            this.concatDirId = "concat-" + UUID.randomUUID();
            storage.createDir(concatDirId);

            this.reduceDestDirId = "reduce-" + UUID.randomUUID();
            storage.createDir(reduceDestDirId);
        }

        private Task createMapTask() {
            return Task.newBuilder()
                       .setTaskId(nextTaskId.getAndIncrement())
                       .setTaskType(Task.TaskType.Map)
                       .setInputDirId(batch.getInputId())
                       .setDestDirId(UUID.randomUUID().toString())
                       .setRNum(batch.getRNum())
                       .addAllTaskBinIds(batch.getMapBinIdsList())
                       .addAllTaskBinIds(List.of((batch.getPartitionBinId())))
                       .build();
        }

        private Task createReduceTask() {
            return Task.newBuilder()
                       .setTaskId(nextTaskId.getAndIncrement())
                       .setTaskType(Task.TaskType.Reduce)
                       .setInputDirId(concatDirId)
                       .setRNum(batch.getRNum())
                       .setDestDirId(reduceDestDirId)
                       .addAllTaskBinIds(batch.getReduceBinIdsList())
                       .build();
        }

        @Override
        public void run() {
            try {

                // Mapping phase

                List<Split> splits = storage.getSplitsForDir(batch.getInputId(), splitCount);
                for (Split split : splits) {
                    var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                    Task task = createMapTask();

                    storage.createDir(task.getDestDirId());

                    var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();
                    basicMap.put(doMapRequest.getTask().getTaskId(), doMapRequest);
                    ListenableFuture<Response> listenableFuture = workerFutureStub.doMap(doMapRequest);
                    runningMaps.computeIfAbsent(doMapRequest.getTask().getTaskId(), r -> new ArrayList<>())
                               .add(listenableFuture);
                    Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver,
                            phaseDoneLatch, doMapRequest, 0, runningMaps, completedMaps), pool);
                }

                rescheduleIfStale(runningMaps, completedMaps, basicMap);

                // Concatenation phase
                List<Integer> fileIds = new ArrayList<>();

                assert !workersDestDirIds.isEmpty();

                for (int i = 0; i < batch.getRNum(); i++) {
                    File mergedFile;
                    mergedFile = Files.createFile(storage.getDirPath(concatDirId).resolve(String.valueOf(i))).toFile();

                    for (var workersDestDirId : workersDestDirIds) {
                        try (var outputStream = new BufferedOutputStream(new FileOutputStream(mergedFile, true))) {
                            File file = storage.getFile(workersDestDirId, i).file();
                            try (var inputStream = new BufferedInputStream(new FileInputStream(file))) {
                                IOUtils.copy(inputStream, outputStream);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    storage.putFile(concatDirId, i, mergedFile);
                    fileIds.add(i);
                }

                // Reduce phase
                LOGGER.info("TM - got to reduce phase.");
                phaseDoneLatch = new CountDownLatch(fileIds.size());

                assert fileIds.size() == batch.getRNum();

                for (var fileId : fileIds) {
                    var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                    Task task = createReduceTask();

                    var doReduceRequest = DoReduceRequest.newBuilder().setTask(task).setFileId(fileId).build();
                    basicReduce.put(doReduceRequest.getTask().getTaskId(), doReduceRequest);
                    ListenableFuture<Response> listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                    runningReduces.computeIfAbsent(doReduceRequest.getTask().getTaskId(), r -> new ArrayList<>())
                                  .add(listenableFuture);
                    Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver,
                            phaseDoneLatch, doReduceRequest, 0, runningReduces, completedReduces), pool);
                }

                rescheduleIfStale(runningReduces, completedReduces, basicReduce);

                storage.moveUniqueReduceResultsToDestDir(reduceDestDirId, batch.getFinalDestDirId());

                Utils.respondWithSuccess(responseObserver);

            } catch (Exception e) {
                Utils.respondWithThrowable(e, responseObserver);
            } finally {
                // Cleanup phase
                for (var dir : workersDestDirIds)
                    Utils.removeDirRecursively(Path.of(dir));
                Utils.removeDirRecursively(storage.getDirPath(concatDirId));
                Utils.removeDirRecursively(storage.getDirPath(reduceDestDirId));
            }
        }

        private <T> void rescheduleIfStale(Map<Long, List<ListenableFuture<Response>>> runningRequests, Map<Long,
                Boolean> completedRequests, Map<Long, T> basicRequest) throws InterruptedException {
            var latchStatus = false;
            while (!latchStatus) {
                latchStatus = phaseDoneLatch.await(WORKER_TIMEOUT, TimeUnit.SECONDS);
                if (!latchStatus) {
                    for (var taskId : runningRequests.keySet()) { // resend still running reduces
                        LOGGER.info("TM - taskID: " + taskId + " is stale resending.");

                        var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                        ListenableFuture<Response> listenableFuture;
                        T request = basicRequest.get(taskId);
                        if (request instanceof DoMapRequest doMapRequest) {
                            listenableFuture = workerFutureStub.doMap(Utils.changeDestDirIdInTask(doMapRequest));
                            runningRequests.computeIfAbsent(doMapRequest.getTask().getTaskId(), r -> new ArrayList<>())
                                           .add(listenableFuture);
                        } else if (request instanceof DoReduceRequest doReduceRequest) {
                            listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                            runningRequests.computeIfAbsent(doReduceRequest.getTask()
                                                                           .getTaskId(), r -> new ArrayList<>())
                                           .add(listenableFuture);
                        } else {
                            throw new AssertionError("Object is neither an instance of DoMapRequest or " +
                                    "DoReduceRequest");
                        }
                        Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver,
                                phaseDoneLatch, request, 0, runningRequests, completedRequests), pool);
                    }
                }
            }
        }

        private FutureCallback<Response> createWorkerResponseCallback(StreamObserver<Response> responseObserver,
                                                                      CountDownLatch operationsDoneLatch,
                                                                      Object request, int attempt, Map<Long,
                List<ListenableFuture<Response>>> runningRequests, Map<Long, Boolean> completedRequests) {

            return new FutureCallback<>() {
                @Override
                public void onSuccess(Response result) {
                    if (result.getStatusCode() == StatusCode.Err) {
                        if (attempt >= MAX_ATTEMPT) {
                            Utils.respondWithThrowable(new RuntimeException("Number of attempts for task was " +
                                    "exceeded"), responseObserver);
                            return;
                        }
                        reRunTask(responseObserver, operationsDoneLatch, attempt + 1);
                    }

                    Long taskId = getTaskIdFromRequestObject(request);

                    if (completedRequests.putIfAbsent(taskId, true) == null) {
                        var futures = runningRequests.remove(taskId);
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
                    if (attempt >= MAX_ATTEMPT) {
                        if (t.getClass() == CancellationException.class) {
                            return;
                        }
                        Utils.respondWithThrowable(t, responseObserver); //TODO jak cancelujemy future to się to
                        // wysyłą i to jest problem
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
                    LOGGER.info("TM - rerunning request. Attempt nr: " + attempt + "| request \n" + request);
                    Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver,
                            operationsDoneLatch, request, attempt + 1, runningRequests, completedRequests), pool);
                }

                private Long getTaskIdFromRequestObject(Object request) {
                    if (request instanceof DoMapRequest doMapRequest) {
                        return doMapRequest.getTask().getTaskId();
                    } else if (request instanceof DoReduceRequest doReduceRequest) {
                        return doReduceRequest.getTask().getTaskId();
                    } else
                        throw new AssertionError("Object is neither an instance of DoMapRequest or " +
                                "DoReduceRequest");
                }
            };
        }

    }
}