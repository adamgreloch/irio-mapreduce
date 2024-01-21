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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase implements HealthCheckable, Serializable {
    public static final Logger LOGGER = LoggerFactory.getLogger(TaskManagerImpl.class);
    private final TaskManagerImpl tmINSTANCE;
    private final Storage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(10);
    private final ManagedChannel workerChannel;
    private final Integer MAX_ATTEMPT = -1;
    private final Integer WORKER_TIMEOUT = 300; // Time after which task will be rerun in seconds.
    private final List<Batch> processingBatches;
    private final Map<Batch, TMStatus> batchTMStatusMap;
    private final Map<Batch, BatchHandler> batchBatchHandlerMap;

    public TaskManagerImpl(Storage storage, HealthStatusManager health, String workersUri) {
        this.storage = storage;
        LOGGER.info("Worker service URI set to: " + workersUri);
        this.workerChannel = Utils.createCustomClientChannelBuilder(workersUri).executor(pool).build();
        this.processingBatches = new ArrayList<>();
        this.batchTMStatusMap = new ConcurrentHashMap<>();
        this.batchBatchHandlerMap = new ConcurrentHashMap<>();
        this.tmINSTANCE = this;
    }

    public static void start() throws IOException, InterruptedException {
        LOGGER.info("Hello from TaskManager!");

        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        HealthStatusManager health = new HealthStatusManager();

        Optional<TaskManagerImpl> tmBackup = storage.retrieveTMState(ClusterConfig.POD_NAME);
        if (tmBackup.isEmpty()) {
            Utils.start_server(new TaskManagerImpl(storage, health, ClusterConfig.WORKERS_URI), health,
                            ClusterConfig.TASK_MANAGERS_URI)
                    .awaitTermination();
        } else {
            TaskManagerImpl tm = tmBackup.get();
            var server = Utils.start_server(tm, health, ClusterConfig.TASK_MANAGERS_URI);

            for (var batch : tm.processingBatches) {
                tm.reRunButch(batch);
            }

            server.awaitTermination();
        }

    }

    @Override
    public void doBatch(Batch batch, StreamObserver<Response> responseObserver) {
        Utils.handleServerBreakerAction(responseObserver);
        processingBatches.add(batch);
        BatchHandler batchHandler = new BatchHandler(batch, responseObserver);
        batchBatchHandlerMap.put(batch, batchHandler);
        batchTMStatusMap.put(batch, TMStatus.BEGINNING);
        pool.execute(batchHandler);
    }

    private void reRunButch(Batch batch) {
        BatchHandler batchHandler = batchBatchHandlerMap.get(batch); //idK MOŻE NIE BYĆ USTAWIONE?
        batchHandler.rerunHalfOfPhase = true;
        batchHandler.concatDirId = UUID.randomUUID().toString();
        pool.execute(batchHandler);
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
        LOGGER.warn("healthchecking not implemented");
        return PingResponse.getDefaultInstance();
    }

    class BatchHandler implements Runnable, Serializable {
        private final Batch batch;
        private final StreamObserver<Response> responseObserver;
        private CountDownLatch phaseDoneLatch;
        private final int splitsNum;
        private final AtomicInteger nextTaskId;
        private final Integer reduceTaskCount;
        private final List<String> workersDestDirIds; // List of directories that belong do batch.
        private String concatDirId;
        // Each Key in this map is a Task.ID
        private final Map<Long, List<ListenableFuture<Response>>> runningMaps;
        private final Map<Long, List<ListenableFuture<Response>>> runningReduces;
        // DO NOT USE basicMap for getting destDirId use workersDestDirIds instead;
        private final Map<Long, DoMapRequest> basicMap; // First instance of request for provided Id from which we should mutate our request if we must redo it.
        private final Map<Long, DoReduceRequest> basicReduce; // First instance of request for provided Id from which we should mutate our request if we must redo it.
        private final Map<Long, Boolean> completedMaps;
        private final Map<Long, Boolean> completedReduces;
        private boolean rerunHalfOfPhase;
        private List<Split> splits;

        BatchHandler(Batch batch, StreamObserver<Response> responseObserver) {
            this.batch = batch;
            this.responseObserver = responseObserver;
            this.splitsNum = 2;
            this.phaseDoneLatch = new CountDownLatch(splitsNum);
            this.nextTaskId = new AtomicInteger(0);
            this.workersDestDirIds = new ArrayList<>();
            this.reduceTaskCount = batch.getReduceBinIdsCount();
            this.runningMaps = new ConcurrentHashMap<>();
            this.runningReduces = new ConcurrentHashMap<>();
            this.completedMaps = new ConcurrentHashMap<>();
            this.completedReduces = new ConcurrentHashMap<>();
            this.basicMap = new ConcurrentHashMap<>();
            this.basicReduce = new ConcurrentHashMap<>();
            this.concatDirId = UUID.randomUUID().toString();
            this.splits = new ArrayList<>();
            this.rerunHalfOfPhase = false;
            storage.createDir(concatDirId);
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

        private Task createReduceTask() {
            return Task.newBuilder()
                    .setTaskId(nextTaskId.getAndIncrement())
                    .setTaskType(Task.TaskType.Reduce)
                    .setInputDirId(concatDirId)
                    .setDestDirId(batch.getFinalDestDirId())
                    .addAllTaskBinIds(batch.getReduceBinIdsList())
                    .build();
        }

        private void saveState(TMStatus status) {
            batchTMStatusMap.put(batch, status);
            storage.saveTMState(tmINSTANCE, ClusterConfig.POD_NAME);
            rerunHalfOfPhase = false;
        }

        private void sendMap(DoMapRequest doMapRequest) {
            var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);
            ListenableFuture<Response> listenableFuture = workerFutureStub.doMap(doMapRequest);
            runningMaps.computeIfAbsent(doMapRequest.getTask().getTaskId(), r -> new ArrayList<>()).add(listenableFuture);
            Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, phaseDoneLatch, doMapRequest, 0, runningMaps, completedMaps), pool);
        }

        private void sendReduce(DoReduceRequest doReduceRequest) {
            var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);
            ListenableFuture<Response> listenableFuture = workerFutureStub.doReduce(doReduceRequest);
            runningReduces.computeIfAbsent(doReduceRequest.getTask().getTaskId(), r -> new ArrayList<>()).add(listenableFuture);
            Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, phaseDoneLatch, doReduceRequest, 0, runningReduces, completedReduces), pool);
        }

        @Override
        public void run() {

            // Mapping phase
            if (!batchTMStatusMap.get(batch).furtherThan(TMStatus.SENT_MAPS)) {
                splits = storage.getSplitsForDir(batch.getInputId(), splitsNum);
                for (Split split : splits) {
                    Task task = createMapTask();

                    storage.createDir(task.getDestDirId());

                    var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();
                    basicMap.put(doMapRequest.getTask().getTaskId(), doMapRequest);
                    sendMap(doMapRequest);
                }
                saveState(TMStatus.SENT_MAPS);
            }

            if (!batchTMStatusMap.get(batch).furtherThan(TMStatus.RESCHEDULED_MAPS_IF_STALE)) {
                if (rerunHalfOfPhase) {
                    var mapsToRerun = runningMaps.keySet();
                    runningMaps.clear();
                    phaseDoneLatch = new CountDownLatch(mapsToRerun.size());
                    for (var taskId : mapsToRerun) {

                        var doMapRequest = Utils.changeDestDirIdInTask(basicMap.get(taskId));
                        sendMap(doMapRequest);
                    }
                }

                try {
                    rescheduleIfStale(runningMaps, completedMaps, basicMap);
                } catch (InterruptedException e) {
                    Utils.respondWithThrowable(e, responseObserver);
                    return;
                }
                saveState(TMStatus.RESCHEDULED_MAPS_IF_STALE);
            }
            List<Integer> fileIds = new ArrayList<>();

            // Concatenation phase
            // If we rerun we must complete this in full, no more staring from middle.
            if (!batchTMStatusMap.get(batch).furtherThan(TMStatus.FINISHED_CONCATENATION)) {
                assert !workersDestDirIds.isEmpty();

                for (int i = 0; i < reduceTaskCount; i++) {
                    File mergedFile = null;
                    try {
                        mergedFile = Files.createFile(storage.getDirPath(concatDirId).resolve(String.valueOf(i))).toFile();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

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

                    storage.putFile(concatDirId, i, mergedFile);
                    fileIds.add(i);
                }
                saveState(TMStatus.FINISHED_CONCATENATION);
            }

            // Reduce phase
            if (!batchTMStatusMap.get(batch).furtherThan(TMStatus.SENT_REDUCES)) {
                phaseDoneLatch = new CountDownLatch(fileIds.size());

                for (var fileId : fileIds) {
                    var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                    Task task = createReduceTask();

                    var doReduceRequest = DoReduceRequest.newBuilder().setTask(task).setFileId(fileId).build();
                    basicReduce.put(doReduceRequest.getTask().getTaskId(), doReduceRequest);
                    ListenableFuture<Response> listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                    runningReduces.computeIfAbsent(doReduceRequest.getTask().getTaskId(), r -> new ArrayList<>()).add(listenableFuture);
                    Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, phaseDoneLatch,
                            doReduceRequest, 0, runningReduces, completedReduces), pool);
                }
                saveState(TMStatus.SENT_REDUCES);
            }

            if (!batchTMStatusMap.get(batch).furtherThan(TMStatus.RESCHEDULED_REDUCES_IF_STALE)) {
                if (rerunHalfOfPhase) {
                    var reducesToRerun = runningReduces.keySet();
                    runningReduces.clear();
                    phaseDoneLatch = new CountDownLatch(reducesToRerun.size());
                    for (var taskId : reducesToRerun) {

                        var doReduceRequest = basicReduce.get(taskId);
                        sendReduce(doReduceRequest);
                    }
                }

                try {
                    rescheduleIfStale(runningReduces, completedReduces, basicReduce);
                } catch (InterruptedException e) {
                    Utils.respondWithThrowable(e, responseObserver);
                    return;
                }
                saveState(TMStatus.RESCHEDULED_REDUCES_IF_STALE);
            }

            if (!batchTMStatusMap.get(batch).furtherThan(TMStatus.FINISHED)) {
                storage.removeReduceDuplicates(batch.getFinalDestDirId());
                processingBatches.remove(batch);

                Utils.respondWithSuccess(responseObserver);
                saveState(TMStatus.FINISHED);
            }
        }

        private <T> void rescheduleIfStale(Map<Long, List<ListenableFuture<Response>>> runningRequests,
                                           Map<Long, Boolean> completedRequests,
                                           Map<Long, T> basicRequest) throws InterruptedException {
            var latchStatus = false;
            while (!latchStatus) {
                latchStatus = phaseDoneLatch.await(WORKER_TIMEOUT, TimeUnit.SECONDS);
                if (!latchStatus) {
                    for (var taskId : runningRequests.keySet()) { // resend still running reduces
                        var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                        ListenableFuture<Response> listenableFuture;
                        T request = basicRequest.get(taskId);
                        if (request instanceof DoMapRequest doMapRequest) {
                            listenableFuture = workerFutureStub.doMap(Utils.changeDestDirIdInTask(doMapRequest));
                            runningRequests.computeIfAbsent(doMapRequest.getTask().getTaskId(), r -> new ArrayList<>()).add(listenableFuture);
                        } else if (request instanceof DoReduceRequest doReduceRequest) {
                            listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                            runningRequests.computeIfAbsent(doReduceRequest.getTask().getTaskId(), r -> new ArrayList<>()).add(listenableFuture);
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
                                                                      Object request,
                                                                      int attempt,
                                                                      Map<Long, List<ListenableFuture<Response>>> runningRequests,
                                                                      Map<Long, Boolean> completedRequests) {

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