package pl.edu.mimuw.mapreduce.worker;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.storage.Storage;
import pl.edu.mimuw.mapreduce.worker.util.Combiner;
import pl.edu.mimuw.mapreduce.worker.util.ConcurrentProcessor;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.worker.DoCombineRequest;
import pl.edu.mimuw.proto.worker.DoWorkRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerImpl extends WorkerGrpc.WorkerImplBase {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.worker");

    private final Storage storage;
    private final ExecutorService pool;

    public WorkerImpl(Storage storage) {
        this.storage = storage;
        this.pool = Executors.newCachedThreadPool();
    }

    @Override
    public void doWork(DoWorkRequest request, StreamObserver<Response> responseObserver) {
        pool.execute(new DoWorkHandler(request, responseObserver));
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        throw new RuntimeException("todo");
    }

    @Override
    public void doCombine(DoCombineRequest request, StreamObserver<Response> responseObserver) {
        pool.execute(new DoCombineHandler(request, responseObserver));
    }

    class DoWorkHandler implements Runnable {
        private final DoWorkRequest request;
        private final StreamObserver<Response> responseObserver;

        DoWorkHandler(DoWorkRequest request, StreamObserver<Response> responseObserver) {
            this.request = request;
            this.responseObserver = responseObserver;
        }

        public void run() {
            var split = request.getSplit();
            var task = request.getTask();

            StatusCode statusCode;

            try (var processor = new ConcurrentProcessor(storage, split, task.getTaskBinIdsList(),
                    task.getDataDirId(), task.getDestinationId())) {
                switch (task.getTaskType()) {
                    case Map -> processor.map();
                    case Reduce -> processor.reduce();
                    case UNRECOGNIZED -> throw new RuntimeException("unrecognized task type");
                }
                statusCode = StatusCode.Ok;
            } catch (Exception e) {
                statusCode = StatusCode.Err;
                logger.log(Level.WARNING, "processing failed: ", e);
            }

            var response = Response.newBuilder().setStatusCode(statusCode).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    class DoCombineHandler implements Runnable {
        private final DoCombineRequest request;
        private final StreamObserver<Response> responseObserver;

        DoCombineHandler(DoCombineRequest request, StreamObserver<Response> responseObserver) {
            this.request = request;
            this.responseObserver = responseObserver;
        }

        public void run() {
            var split1 = request.getSplit1();
            var split2 = request.getSplit2();
            var binary = storage.getFile(Storage.BINARY_DIR, request.getCombineBinId()).file();
            var combiner = new Combiner(storage, request.getDestDirId(), request.getInputDirId(), binary);

            StatusCode statusCode = StatusCode.Ok;

            combiner.combine(split1, split2);

            var response = Response.newBuilder().setStatusCode(statusCode).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
