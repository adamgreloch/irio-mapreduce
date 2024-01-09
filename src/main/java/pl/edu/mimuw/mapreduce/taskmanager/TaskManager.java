package pl.edu.mimuw.mapreduce.taskmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.config.NetworkConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.proto.common.*;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.taskmanager.TaskManagerGrpc;
import pl.edu.mimuw.proto.worker.CombineRequest;
import pl.edu.mimuw.proto.worker.DoWorkRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskManager {
    public static void main(String[] args) throws IOException, InterruptedException {
        //Utils.start_service(new TaskManagerImpl(), 2137);
    }

    public static void start(int port) throws IOException, InterruptedException {
        //Utils.start_service(new TaskManagerImpl(), port);
    }

    static class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase {
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

        @Override
        public void doTask(Task task, StreamObserver<Response> responseObserver) {
            pool.execute(new Handler(task, responseObserver));
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
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
                String hostname;
                int port;

                if (NetworkConfig.IS_KUBERNETES) {
                    hostname = NetworkConfig.WORKERS_HOST;
                    port = NetworkConfig.WORKERS_PORT;
                } else {
                    hostname = "localhost";
                    port = 2122;
                }

                var splits = storage.getSplitsForDir(task.getDataDirId(), splitsNum);

                for (var split : splits) {
                    var managedChannel =
                            ManagedChannelBuilder.forAddress(hostname, port).executor(pool).usePlaintext().build();
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

                        //TODO we must sort splits if they aren't sorted.
                        Queue<Split> splitQueue = new LinkedList<>();
                        List<Future<Response>> futures = new ArrayList<>();

                        while (splitQueue.size() > 1) {
                            var phaseSize = splitQueue.size();
                            if (phaseSize % 2 != 0) phaseSize--;

                            for (int i = 0; i < phaseSize; i += 2) {
                                Split s1 = splitQueue.poll();
                                Split s2 = splitQueue.poll();

                                var managedChannel =
                                        ManagedChannelBuilder.forAddress(hostname, port).executor(pool).usePlaintext().build();
                                var workerFutureStub = WorkerGrpc.newFutureStub(managedChannel);

                                var combineRequest = CombineRequest.newBuilder()
                                        .setCombineBinId(task.getTaskBinIds(1))
                                        .setDestDirId(task.getDataDirId())
                                        .setSplit1(s1)
                                        .setSplit2(s2)
                                        .build();

                                ListenableFuture<Response> listenableFuture = workerFutureStub.doCombine(combineRequest);
                                futures.add(listenableFuture);

                                splitQueue.add(mergeSplits(s1, s2));
                            }
                            for (var future : futures) {
                                try {
                                    var workerResponse = future.get();
                                    if (workerResponse.getStatusCode() == StatusCode.Err) {
                                        throw new RuntimeException("TODO implement this");
                                    }

                                } catch (ExecutionException e) {
                                    throw new RuntimeException(e);// TODO handle this
                                }
                            }
                            futures.clear();
                        }
                        // If reduce was done, it is now necessary to combine the partial results from
                        // all splits to one file, so it represents the result of the whole input.
                        // TaskManager can orchestrate the process by assigning Combine tasks to
                        // workers in a way that achieves a logarithmic complexity.

                        // ...TODO
                    }

                    response = Response.newBuilder().setStatusCode(StatusCode.Ok).build();
                } catch (InterruptedException e) {
                    response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(e.getMessage()).build();
                }

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        private Split mergeSplits(Split s1, Split s2) {
            if (s1.getBeg() < s2.getBeg()) {
                return Split.newBuilder()
                        .setSplitId(s1.getSplitId())
                        .setBeg(s1.getBeg())
                        .setEnd(s2.getEnd())
                        .build();
            } else {
                return Split.newBuilder()
                        .setSplitId(s2.getSplitId())
                        .setBeg(s2.getBeg())
                        .setEnd(s1.getEnd())
                        .build();
            }
        }
    }
}
