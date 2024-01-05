package pl.edu.mimuw.mapreduce.worker;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.storage.local.FileRep;
import pl.edu.mimuw.mapreduce.storage.local.LocalStorage;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.worker.DoWorkRequest;
import pl.edu.mimuw.proto.worker.WorkerGrpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Worker {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.worker");

    public static void main(String[] args) throws IOException, InterruptedException {
        LocalStorage storage = new LocalStorage();
        Utils.start_service(new WorkerImpl(storage), 50042);
    }

    static class WorkerImpl extends WorkerGrpc.WorkerImplBase {

        // TODO: use a more general interface for storage
        private final LocalStorage storage;
        private final ExecutorService pool;

        public WorkerImpl(LocalStorage storage) {
            this.storage = storage;
            this.pool = Executors.newCachedThreadPool();
        }

        @Override
        public void doWork(DoWorkRequest request, StreamObserver<Response> responseObserver) {
            pool.execute(new Handler(request, responseObserver));
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }

        class Handler implements Runnable {
            private final DoWorkRequest request;
            private final StreamObserver<Response> responseObserver;
            private final ArrayList<Future<Void>> futures = new ArrayList<>();

            Handler(DoWorkRequest request, StreamObserver<Response> responseObserver) {
                this.request = request;
                this.responseObserver = responseObserver;
            }

            public void run() {
                var split = request.getSplit();
                var task = request.getTask();

                StatusCode statusCode;

                try (Processor processor = new Processor(storage, task.getBinId(),
                        request.getDestinationId())) {
                    for (Iterator<FileRep> it = storage.get_split_iterator(split); it.hasNext(); ) {
                        FileRep fr = it.next();

                        futures.add(pool.submit(() -> {
                            processor.process_file(fr);
                            return null;
                        }));
                    }

                    for (var fut : futures) {
                        fut.get();
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
    }
}
