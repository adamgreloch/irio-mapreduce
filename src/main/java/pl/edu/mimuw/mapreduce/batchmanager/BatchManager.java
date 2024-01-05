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
        private final ExecutorService executorService = Executors.newCachedThreadPool();

        // Get next Task to be done in batch if it exists. Returns empty optional if there is no more tasks.
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
                // onSuccess process send next Task to TaskManager.
                @Override
                public void onSuccess(Response result) {
                    if (result.getStatusCode() != StatusCode.Ok) {
                        Response response = Response.newBuilder().setStatusCode(result.getStatusCode())
                                .setMessage("Success but got some internal error.").build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
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
                // Stop processing batch and send error message.
                @Override
                public void onFailure(Throwable t) {
                    Response response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(t.getMessage()).build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void doBatch(Batch batch, StreamObserver<Response> responseObserver) {
            ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("localhost", 2137)
                    .executor(executorService).usePlaintext().build();
            var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(managedChannel);

            doneTasks.put(batch, 0);
            finishedMapping.put(batch, false);


            Optional<Task> optionalTask = getTask(batch);
            if (optionalTask.isEmpty()) {
                responseObserver.onNext(Response.newBuilder().setStatusCode(StatusCode.EMPTY_MAP_AND_REDUCE).setMessage("No Maps or Reduces to be processed.").build());
                responseObserver.onCompleted();
                return;
            }

            ListenableFuture<Response> listenableFuture = taskManagerFutureStub.doTask(optionalTask.get());

            Futures.addCallback(listenableFuture, createCallback(batch, responseObserver, taskManagerFutureStub), executorService);
            // Both onNext and onCompleted are called in above function.
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            PingResponse pingResponse = PingResponse.newBuilder()
                    .setStatusCode(HealthStatusCode.Healthy)
                    .build();
            responseObserver.onNext(pingResponse);
            responseObserver.onCompleted();
        }
    }
}
