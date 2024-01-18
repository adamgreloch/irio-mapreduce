package pl.edu.mimuw.mapreduce.batchmanager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.config.ClusterConfig;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.taskmanager.TaskManagerImpl;
import pl.edu.mimuw.proto.batchmanager.BatchManagerGrpc;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
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
import java.util.logging.Logger;

public class BatchManagerImpl extends BatchManagerGrpc.BatchManagerImplBase {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.batchmanagerimpl");

    enum BatchPhase {
        Mapping, Reducing
    }

    public static void start() throws IOException, InterruptedException {
        Utils.start_service(new BatchManagerImpl(), ClusterConfig.BATCH_MANAGERS_PORT);
    }

    public BatchManagerImpl() {}

    private final AtomicInteger taskCount = new AtomicInteger(0);
    private final Map<Batch, Integer> doneTasks = new ConcurrentHashMap<>();
    private final Map<Batch, BatchPhase> batchPhases = new ConcurrentHashMap<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ManagedChannel taskManagerChannel =
            ManagedChannelBuilder.forAddress(ClusterConfig.TASK_MANAGERS_HOST,
            ClusterConfig.TASK_MANAGERS_PORT).executor(pool).usePlaintext().build();

    /**
     * Get next Task to be done in batch if it exists. Returns empty optional if there is no more tasks.
     */
    private Optional<Task> getTask(Batch batch) {
        var builder = Task.newBuilder();
        builder.setTaskId(taskCount.getAndIncrement());

        int nextTaskNr = doneTasks.get(batch);

        switch (batchPhases.get(batch)) {
            case Mapping -> {
                if (nextTaskNr >= batch.getMapBinIdsCount()) {
                    return Optional.empty();
                }
                //TODO set begin from id, by checking storage on whether some maps were already completed for
                // provided batch.
                // For example if error occurred and we need to pick up work from other taskManager.
                for (int i = 0; i < batch.getMapBinIdsCount(); i++) {
                    builder.setTaskBinIds(i, batch.getMapBinIds(i));
                }
            }
            case Reducing -> {
                if (nextTaskNr >= batch.getReduceBinIdsCount()) {
                    return Optional.empty();
                }
                builder.setTaskBinIds(0, batch.getReduceBinIds(nextTaskNr)).setTaskType(Task.TaskType.Reduce);
            }
        }

        return Optional.of(builder.build());
    }

    private FutureCallback<Response> createCallback(Batch batch, StreamObserver<Response> responseObserver,
                                                    TaskManagerGrpc.TaskManagerFutureStub taskManagerFutureStub) {
        return new FutureCallback<>() {
            /** Try to send next Task to TaskManager. */
            @Override
            public void onSuccess(Response result) {
                if (result.getStatusCode() != StatusCode.Ok) {
                    Response response = Response.newBuilder().setStatusCode(result.getStatusCode()).setMessage(
                            "Success but got some internal error.").build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                // TODO check if every map was executed on input, by checking some value in storage.
                // If so set BatchPhase to Reducing.
                int doneTaskNr = doneTasks.get(batch);
                doneTaskNr++;

                if (batchPhases.get(batch).equals(BatchPhase.Mapping)) {
                    // end of mapping phase
                    doneTasks.put(batch, 0);
                    batchPhases.put(batch, BatchPhase.Reducing);
                } else {
                    doneTasks.put(batch, doneTaskNr);
                }

                Optional<Task> optional = getTask(batch);
                if (optional.isEmpty()) {
                    // batch is finished
                    responseObserver.onNext(Response.newBuilder().setStatusCode(StatusCode.Ok).build());
                    responseObserver.onCompleted();
                    return;
                }

                ListenableFuture<Response> listenableFuture = taskManagerFutureStub.doTask(optional.get());
                Futures.addCallback(listenableFuture, createCallback(batch, responseObserver, taskManagerFutureStub),
                        pool);
            }

            /** Stop processing the batch and send error message. */
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
        var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(taskManagerChannel);

        doneTasks.put(batch, 0);
        batchPhases.put(batch, BatchPhase.Mapping);

        // Assign first task from batch
        Optional<Task> optionalTask = getTask(batch);
        if (optionalTask.isEmpty()) {
            responseObserver.onNext(Response.newBuilder().setStatusCode(StatusCode.Err).setMessage("No Maps or " +
                    "Reduces to be processed.").build());
            responseObserver.onCompleted();
            return;
        }

        ListenableFuture<Response> listenableFuture = taskManagerFutureStub.doTask(optionalTask.get());

        Futures.addCallback(listenableFuture, createCallback(batch, responseObserver, taskManagerFutureStub),
                pool);
        // Both onNext and onCompleted are called in above function.
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        var taskManagerFutureStub = TaskManagerGrpc.newFutureStub(taskManagerChannel);

        var listenableFuture = taskManagerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver,
                MissingConnectionWithLayer.TaskManager), pool);
    }
}
