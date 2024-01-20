package pl.edu.mimuw.mapreduce.worker;

import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.common.HealthCheckable;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.worker.util.Either;
import pl.edu.mimuw.mapreduce.worker.util.MapProcessor;
import pl.edu.mimuw.mapreduce.worker.util.ReduceProcessor;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.worker.DoMapRequest;
import pl.edu.mimuw.proto.worker.DoReduceRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static pl.edu.mimuw.proto.common.Task.TaskType.Map;
import static pl.edu.mimuw.proto.common.Task.TaskType.Reduce;

public class WorkerImpl extends WorkerGrpc.WorkerImplBase implements HealthCheckable {
    private final Storage storage;
    private final ExecutorService pool;
    private final HealthStatusManager health;

    public static void start() throws IOException, InterruptedException {
        Utils.LOGGER.info("Hello from Worker!");

        Storage storage = new DistrStorage(ClusterConfig.STORAGE_DIR);
        HealthStatusManager health = new HealthStatusManager();

        Utils.start_server(new WorkerImpl(storage, health), health, ClusterConfig.WORKERS_URI).awaitTermination();
    }

    public WorkerImpl(Storage storage, HealthStatusManager health) {
        this.storage = storage;
        this.pool = Executors.newCachedThreadPool();
        this.health = health;
    }

    @Override
    public void doMap(DoMapRequest request, StreamObserver<Response> responseObserver) {
        Utils.handleServerBreakerAction(responseObserver);
        pool.execute(new RequestHandler(Either.left(request), responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        if(Utils.handleServerBreakerHealthCheckAction(responseObserver)){
            return;
        }
        Utils.LOGGER.trace("Received health check request");
        Utils.respondToHealthcheck(responseObserver, HealthStatusCode.Healthy);
    }

    @Override
    public void doReduce(DoReduceRequest request, StreamObserver<Response> responseObserver) {
        Utils.handleServerBreakerAction(responseObserver);
        pool.execute(new RequestHandler(Either.right(request), responseObserver));
    }

    @Override
    public PingResponse internalHealthcheck() {
        // TODO perform filesystem access check
        if (Utils.handleServerBreakerInternalHealthCheckAction()){
            return PingResponse.newBuilder().setStatusCode(HealthStatusCode.Error).build();
        }
        return PingResponse.newBuilder().setStatusCode(HealthStatusCode.Healthy).build();
    }

    class RequestHandler implements Runnable {
        private final Either<DoMapRequest, DoReduceRequest> eitherMapOrReduce;
        private final StreamObserver<Response> responseObserver;

        RequestHandler(Either<DoMapRequest, DoReduceRequest> eitherMapOrReduce, StreamObserver<Response> responseObserver) {
            this.eitherMapOrReduce = eitherMapOrReduce;
            this.responseObserver = responseObserver;
        }

        private void processMap(DoMapRequest request) {
            var split = request.getSplit();
            var task = request.getTask();

            try (var processor = new MapProcessor(storage, split, task.getTaskBinIdsList(),
                    task.getInputDirId(), task.getDestDirId())) {
                if (task.getTaskType() != Map) throw new RuntimeException("Bad task type");

                Utils.LOGGER.trace("Performing map");
                processor.map();

                Utils.respondWithSuccess(responseObserver);
            } catch (Exception e) {
                Utils.respondWithThrowable(e, responseObserver);
            }
        }

        public void processReduce(DoReduceRequest request) {
            var file = request.getFileId();
            var task = request.getTask();

            try (var processor = new ReduceProcessor(storage, file, task.getTaskBinIdsList(),
                    task.getInputDirId(), task.getDestDirId())) {
                if (task.getTaskType() != Reduce) throw new RuntimeException("Bad task type");

                Utils.LOGGER.trace("Performing reduce");
                processor.reduce();

                Utils.respondWithSuccess(responseObserver);
            } catch (Exception e) {
                Utils.respondWithThrowable(e, responseObserver);
            }
        }

        public void run() {
            health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
            eitherMapOrReduce.apply(this::processMap, this::processReduce);
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        }
    }
}
