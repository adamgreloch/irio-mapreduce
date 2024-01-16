package pl.edu.mimuw.mapreduce.taskmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.config.NetworkConfig;
import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.taskmanager.TaskManagerGrpc;
import pl.edu.mimuw.proto.worker.DoCombineRequest;
import pl.edu.mimuw.proto.worker.DoWorkRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.taskmanager");

    public static void start(int port) throws IOException, InterruptedException {
        //Utils.start_service(new TaskManagerImpl(), port);
    }

    private final Storage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();

    public TaskManagerImpl(Storage storage) {
        this.storage = storage;
    }

    private FutureCallback<Response> createWorkerResponseCallback(Task task,
                                                                  StreamObserver<Response> responseObserver,
                                                                  WorkerGrpc.WorkerFutureStub workerFutureStub,
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

    private Pair<String, Integer> getHostPort() {
        String hostname;
        int port;

        if (NetworkConfig.IS_KUBERNETES) {
            hostname = NetworkConfig.WORKERS_HOST;
            port = NetworkConfig.WORKERS_PORT;
        } else {
            hostname = "localhost";
            port = 2122;
        }

        return Pair.of(hostname, port);
    }

    @Override
    public void doTask(Task task, StreamObserver<Response> responseObserver) {
        pool.execute(new Handler(task, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        var hostPort = getHostPort();

        var managedChannel = ManagedChannelBuilder.forAddress(hostPort.getLeft(), hostPort.getRight()).executor(pool).usePlaintext().build();
        var workerFutureStub = WorkerGrpc.newFutureStub(managedChannel);

        var listenableFuture = workerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver, MissingConnectionWithLayer.Worker), pool);

    }

    class Handler implements Runnable {
        private final Task task;
        private final StreamObserver<Response> responseObserver;
        private final CountDownLatch operationsDoneLatch;
        private final boolean isReduce;
        private final int splitsNum;

        Handler(Task task, StreamObserver<Response> responseObserver) {
            this.task = task;
            this.responseObserver = responseObserver;
            this.isReduce = task.getTaskType().equals(Task.TaskType.Reduce);

            // TODO dynamic number of splits, either:
            //  a) given by master (as a form of throughput optimization)
            //  b) taken from k8s API
            this.splitsNum = 10;
            this.operationsDoneLatch = new CountDownLatch(splitsNum);
        }

        public void run() {
            var hostPort = getHostPort();


            var splits = storage.getSplitsForDir(task.getDataDirId(), splitsNum);

            for (var split : splits) {
                var managedChannel =
                        ManagedChannelBuilder.forAddress(hostPort.getLeft(), hostPort.getRight()).executor(pool).usePlaintext().build();
                var workerFutureStub = WorkerGrpc.newFutureStub(managedChannel);

                var doWorkRequest = DoWorkRequest.newBuilder().setTask(task).setSplit(split).build();

                ListenableFuture<Response> listenableFuture = workerFutureStub.doWork(doWorkRequest);
                Futures.addCallback(listenableFuture, createWorkerResponseCallback(task, responseObserver,
                        workerFutureStub, operationsDoneLatch), pool);
            }

            Response response;

            try {
                operationsDoneLatch.await();

                if (isReduce) {
                    // If reduce was done, it is now necessary to combine the partial results from
                    // all splits to one file, so it represents the result of the whole input.
                    // TaskManager can orchestrate the process by assigning Combine tasks to
                    // workers in a way that achieves a logarithmic complexity.

                    Queue<SplitBuilder> splitQueue = new LinkedList<>();
                    for (var split : splits)
                        splitQueue.add(new SplitBuilder(split));
                    List<Future<Response>> futures = new ArrayList<>();

                    while (splitQueue.size() > 1) {
                        var phaseSize = splitQueue.size();
                        if (phaseSize % 2 != 0) phaseSize--;

                        for (int i = 0; i < phaseSize; i += 2) {
                            var s1 = splitQueue.poll();
                            var s2 = splitQueue.poll();

                            var managedChannel =
                                    ManagedChannelBuilder.forAddress(hostPort.getLeft(), hostPort.getRight()).executor(pool).usePlaintext().build();
                            var workerFutureStub = WorkerGrpc.newFutureStub(managedChannel);

                            assert s1 != null;
                            assert s2 != null;
                            var combineRequest =
                                    DoCombineRequest.newBuilder().setCombineBinId(task.getTaskBinIds(1)).setDestDirId(task.getDataDirId()).setSplit1(s1.build()).setSplit2(s2.build()).build();

                            ListenableFuture<Response> listenableFuture = workerFutureStub.doCombine(combineRequest);
                            futures.add(listenableFuture);

                            splitQueue.add(SplitBuilder.merge(s1, s2));
                        }
                        for (var future : futures) {
                            var workerResponse = future.get();
                        }
                        futures.clear();
                    }
                }

                response = Response.newBuilder().setStatusCode(StatusCode.Ok).build();
            } catch (Exception e) {
                response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(e.getMessage()).build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}