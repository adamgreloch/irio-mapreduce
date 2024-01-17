package pl.edu.mimuw.mapreduce.taskmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.config.ClusterConfig;
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
import java.util.logging.Logger;

public class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.taskmanager");

    public static void start() throws IOException, InterruptedException {
        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        Utils.start_service(new TaskManagerImpl(storage), ClusterConfig.TASK_MANAGERS_PORT);
    }

    private final Storage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ManagedChannel workerChannel =
            ManagedChannelBuilder.forAddress(ClusterConfig.WORKERS_HOST,
                    ClusterConfig.WORKERS_PORT).executor(pool).usePlaintext().build();

    public TaskManagerImpl(Storage storage) {
        this.storage = storage;
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
                Response response =
                        Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(t.getMessage()).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void doBatch(Batch batch, StreamObserver<Response> responseObserver) {
        pool.execute(new Handler(batch, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

        var listenableFuture = workerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver,
                MissingConnectionWithLayer.Worker), pool);

    }

    class Handler implements Runnable {
        private final Batch batch;
        private final StreamObserver<Response> responseObserver;
        private CountDownLatch operationsDoneLatch;
        private final int splitsNum;
        private final AtomicInteger nextTaskId;
        private final Integer R_COUNT;
        private final List<String> dirBatch; //List of directories that belong do batch.
        private final String TMWorkingDir;

        Handler(Batch batch, StreamObserver<Response> responseObserver) {
            this.batch = batch;
            this.responseObserver = responseObserver;
            this.splitsNum = 10;
            this.operationsDoneLatch = new CountDownLatch(splitsNum);
            this.nextTaskId = new AtomicInteger(0);
            this.dirBatch = new ArrayList<>();
            this.R_COUNT = batch.getReduceBinIdsCount();
            this.TMWorkingDir = UUID.randomUUID().toString();
        }

        private Task createMapTask(Split split) {
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

            //Mapping phase
            List<Split> splits = storage.getSplitsForDir(batch.getInputId(), splitsNum);

            for (Split split : splits) {
                var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                Task task = createMapTask(split);
                dirBatch.add(task.getDestDirId());

                var doMapRequest = DoMapRequest.newBuilder().setTask(task).setSplit(split).build();
                ListenableFuture<Response> listenableFuture = workerFutureStub.doMap(doMapRequest);
                Futures.addCallback(listenableFuture,
                        createWorkerResponseCallback(responseObserver, operationsDoneLatch),
                        pool);
            }

            //Concatenation phase

            try {
                operationsDoneLatch.await();
            } catch (InterruptedException e) {
                var response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(e.getMessage()).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }

            List<Integer> fileIDs = new ArrayList<>();

            //assert if dirbatch is not empty
            for (int i = 0; i < R_COUNT; i++) {
                File mergedFile = storage.getFile(dirBatch.get(0), i).file();

                //TODO wyabstrachować to do funkcji
                OutputStream outputStream = null;
                try {
                    outputStream = new BufferedOutputStream(new FileOutputStream(mergedFile, true));
                    for (int j = 1; j < dirBatch.size(); j++) {
                        InputStream inputStream = null;
                        try {
                            File file = storage.getFile(dirBatch.get(j), i).file();
                            inputStream = new BufferedInputStream(new FileInputStream(file));
                            IOUtils.copy(inputStream, outputStream);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            IOUtils.closeQuietly(inputStream);
                        }
                    }
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } finally {
                    IOUtils.closeQuietly(outputStream);
                }
                //Nie powinno być kolizji id bo jesteśmy jedynimi którzy wkładają do tego konkretnego dest.
                storage.putFile(TMWorkingDir, i, mergedFile);
                fileIDs.add(i);
            }

            //Reduce phase
            operationsDoneLatch = new CountDownLatch(splitsNum);

            for (var fileId : fileIDs) {
                var workerFutureStub = WorkerGrpc.newFutureStub(workerChannel);

                Task task = createReduceTask(TMWorkingDir);

                var doReduceRequest = DoReduceRequest.newBuilder().setTask(task).setFileId(fileId).build();
                ListenableFuture<Response> listenableFuture = workerFutureStub.doReduce(doReduceRequest);
                Futures.addCallback(listenableFuture, createWorkerResponseCallback(responseObserver, operationsDoneLatch), pool);
            }

            try {
                operationsDoneLatch.await();
            } catch (InterruptedException e) {
                var response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(e.getMessage()).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            var response = Response.newBuilder().setStatusCode(StatusCode.Ok).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}