package pl.edu.mimuw.mapreduce.batchmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.batchmanager.BatchManagerGrpc;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.taskmanager.TaskManagerGrpc;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchManager {

    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new BatchManagerImpl(), 50043);
    }

    static class BatchManagerImpl extends BatchManagerGrpc.BatchManagerImplBase {
        private final AtomicInteger taskCount = new AtomicInteger(0);
        private final Map<Batch, Integer> doneTasks = new ConcurrentHashMap<>();
        private final Map<Batch, Boolean> finishedMapping = new ConcurrentHashMap<>();
        private final ExecutorService executorService = Executors.newFixedThreadPool(10);
        private StatusCode statusCode = StatusCode.Ok;

        //Zwracamy kolejnego taska do zrobienia w danym batchu, jeśli jakiś istnieje.
        private Optional<Task> getTask(Batch batch) {
            var builder = Task.newBuilder();
            builder.setTaskId(taskCount.getAndIncrement());

            int nextTaskNr = doneTasks.get(batch);

            if (finishedMapping.get(batch)) { // we now Reduce
                if (nextTaskNr >= batch.getReduceBinIdsCount()) {
                    return Optional.empty();
                }
                builder.setBinId(batch.getReduceBinIds(nextTaskNr))
                        .setTaskType(Task.TaskType.Reduce);
                return Optional.of(builder.build());
            }

            // we now Map
            if (nextTaskNr >= batch.getMapBinIdsCount()) {
                return Optional.empty();
            }
            builder.setBinId(batch.getMapBinIds(nextTaskNr))
                    .setTaskType(Task.TaskType.Map);
            return Optional.of(builder.build());
        }

        private FutureCallback<Response> createCallback(Batch batch,
                                                        StreamObserver<Response> responseObserver,
                                                        TaskManagerGrpc.TaskManagerFutureStub taskManagerFutureStub) {
            return new FutureCallback<>() {
                @Override
                public void onSuccess(Response result) {
                    System.out.println("Success--------------");
                    if (result.getStatusCode() != StatusCode.Ok) {
                        statusCode = result.getStatusCode();
                        return;
                    }

                    int doneTaskNr = doneTasks.get(batch);
                    doneTaskNr++;

                    if (batch.getMapBinIdsCount() <= doneTaskNr) {
                        doneTasks.put(batch, 0);
                        finishedMapping.put(batch, true);
                    } else {
                        doneTasks.put(batch, doneTaskNr);
                    }

                    Optional<Task> optional = getTask(batch);
                    if (optional.isEmpty()) {
                        responseObserver.onNext(Response.newBuilder().setStatusCode(StatusCode.Ok).build());
                        responseObserver.onCompleted();
                        return;
                    }

                    ListenableFuture<Response> listenableFuture = taskManagerFutureStub.doTask(optional.get());
                    Futures.addCallback(listenableFuture, createCallback(batch, responseObserver, taskManagerFutureStub), executorService);
                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("DUPA---------------------");
                    System.out.println(t.getMessage());
                    statusCode = StatusCode.Err;
                    Response response = Response.newBuilder().setStatusCode(statusCode).build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void doBatch(Batch batch, StreamObserver<Response> responseObserver) {
            ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("localhost", 2137)
                    .executor(executorService).usePlaintext().build(); // TODO ustawić to dobrze.
            var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(managedChannel);

            doneTasks.put(batch, 0);
            finishedMapping.put(batch, false);


            Optional<Task> optionalTask = getTask(batch);
            if (optionalTask.isEmpty()) {
                responseObserver.onNext(Response.newBuilder().setStatusCode(StatusCode.EMPTY_MAP_AND_REDUCE).build());
                responseObserver.onCompleted();
                return;
            }

            ListenableFuture<Response> listenableFuture = taskManagerFutureStub.doTask(optionalTask.get());

            Futures.addCallback(listenableFuture, createCallback(batch, responseObserver, taskManagerFutureStub), executorService);
            // onNext i onCompleted jest wołane w createCallback.
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            PingResponse pingResponse = PingResponse.newBuilder()
                    .setStatusCode(statusCode == StatusCode.Ok ? HealthStatusCode.Healthy : HealthStatusCode.Error)
                    .build(); // Warto by było się zastanowić jak z tymi kodami

            responseObserver.onNext(pingResponse);
            responseObserver.onCompleted();
        }
    }
}
