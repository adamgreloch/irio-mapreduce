package pl.edu.mimuw.mapreduce.worker;

import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.mapreduce.common.HealthCheckable;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.storage.local.DistrStorage;
import pl.edu.mimuw.mapreduce.worker.util.ConcurrentMapProcessor;
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
import java.util.logging.Level;

import static pl.edu.mimuw.proto.common.Task.TaskType.Map;
import static pl.edu.mimuw.proto.common.Task.TaskType.Reduce;

public class WorkerImpl extends WorkerGrpc.WorkerImplBase implements HealthCheckable {
    private final Storage storage;
    private final ExecutorService pool;
    private final HealthStatusManager health;

    public static void start() throws IOException, InterruptedException {
        Utils.LOGGER.log(Level.INFO, "Hello from Worker!");

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
        pool.execute(new DoMapHandler(request, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        Utils.LOGGER.log(Level.FINE, "Received health check request");

        PingResponse pingResponse = PingResponse.newBuilder().setStatusCode(HealthStatusCode.Healthy).build();
        responseObserver.onNext(pingResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void doReduce(DoReduceRequest request, StreamObserver<Response> responseObserver) {
        pool.execute(new DoReduceHandler(request, responseObserver));
    }

    @Override
    public PingResponse internalHealthcheck() {
        Utils.LOGGER.log(Level.SEVERE, "healthchecking not implemented");
        return PingResponse.getDefaultInstance();
    }

    class DoMapHandler implements Runnable {
        private final DoMapRequest request;
        private final StreamObserver<Response> responseObserver;

        DoMapHandler(DoMapRequest request, StreamObserver<Response> responseObserver) {
            this.request = request;
            this.responseObserver = responseObserver;
        }

        public void run() {
            health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
            var split = request.getSplit();
            var task = request.getTask();

            try (var processor = new ConcurrentMapProcessor(storage, split, task.getTaskBinIdsList(),
                    task.getInputDirId(), task.getDestDirId())) {

                Utils.LOGGER.log(Level.FINE, "performing map");

                if (task.getTaskType() != Map) throw new RuntimeException("bad task type");

                processor.map();
            } catch (Exception e) {
                Utils.respondWithThrowable(e, responseObserver);
            }

            Utils.respondWithSuccess(responseObserver);
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        }
    }

    class DoReduceHandler implements Runnable {
        private final DoReduceRequest request;
        private final StreamObserver<Response> responseObserver;

        DoReduceHandler(DoReduceRequest request, StreamObserver<Response> responseObserver) {
            this.request = request;
            this.responseObserver = responseObserver;
        }

        public void run() {
            health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
            var file = request.getFileId();
            var task = request.getTask();

            try {
                if (task.getTaskType() != Reduce) throw new RuntimeException("bad task type");

                Utils.LOGGER.log(Level.FINE, "performing reduce");

                // TODO perform reduce

                Utils.LOGGER.log(Level.WARNING, "reducing todo");

            } catch (Exception e) {
                Utils.respondWithThrowable(e, responseObserver);
            }

            Utils.respondWithSuccess(responseObserver);
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        }
    }
}
