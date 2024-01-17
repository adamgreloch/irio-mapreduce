package pl.edu.mimuw.mapreduce.worker;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.worker.util.ConcurrentMapProcessor;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.healthcheck.HealthStatusCode;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.worker.DoMapRequest;
import pl.edu.mimuw.proto.worker.DoReduceRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import static pl.edu.mimuw.proto.common.Task.TaskType.Map;
import static pl.edu.mimuw.proto.common.Task.TaskType.Reduce;

import static pl.edu.mimuw.proto.common.Task.TaskType.Map;
import static pl.edu.mimuw.proto.common.Task.TaskType.Reduce;

public class WorkerImpl extends WorkerGrpc.WorkerImplBase {
    private final Storage storage;
    private final ExecutorService pool;

    public WorkerImpl(Storage storage) {
        this.storage = storage;
        this.pool = Executors.newCachedThreadPool();
    }

    @Override
    public void doMap(DoMapRequest request, StreamObserver<Response> responseObserver) {
        pool.execute(new DoMapHandler(request, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        PingResponse pingResponse = PingResponse.newBuilder().setStatusCode(HealthStatusCode.Healthy).build();
        responseObserver.onNext(pingResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void doReduce(DoReduceRequest request, StreamObserver<Response> responseObserver) {
        pool.execute(new DoReduceHandler(request, responseObserver));
    }

    class DoMapHandler implements Runnable {
        private final DoMapRequest request;
        private final StreamObserver<Response> responseObserver;

        DoMapHandler(DoMapRequest request, StreamObserver<Response> responseObserver) {
            this.request = request;
            this.responseObserver = responseObserver;
        }

        public void run() {
            var split = request.getSplit();
            var task = request.getTask();

            StatusCode statusCode;
            String message = "";

            try (var processor = new ConcurrentMapProcessor(storage, split, task.getTaskBinIdsList(),
                    task.getInputDirId(), task.getDestDirId())) {

                Utils.LOGGER.log(Level.FINE, "performing map");

                if (task.getTaskType() != Map)
                    throw new RuntimeException("bad task type");

                processor.map();

                statusCode = StatusCode.Ok;
            } catch (Exception e) {
                statusCode = StatusCode.Err;
                message = e.toString();
                Utils.LOGGER.log(Level.WARNING, "processing failed: ", e);
            }

            var response = Response.newBuilder().setStatusCode(statusCode).setMessage(message).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    static class DoReduceHandler implements Runnable {
        private final DoReduceRequest request;
        private final StreamObserver<Response> responseObserver;

        DoReduceHandler(DoReduceRequest request, StreamObserver<Response> responseObserver) {
            this.request = request;
            this.responseObserver = responseObserver;
        }

        public void run() {
            var file = request.getFileId();
            var task = request.getTask();

            StatusCode statusCode;
            String message = "";

            try {
                if (task.getTaskType() != Reduce)
                    throw new RuntimeException("bad task type");

                Utils.LOGGER.log(Level.FINE, "performing reduce");

                // TODO perform reduce
                Utils.LOGGER.log(Level.WARNING, "reducing todo");

                statusCode = StatusCode.Ok;
            } catch (Exception e) {
                statusCode = StatusCode.Err;
                message = e.toString();
                Utils.LOGGER.log(Level.SEVERE, "processing failed: ", e);
            }

            var response = Response.newBuilder().setStatusCode(statusCode).setMessage(message).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
