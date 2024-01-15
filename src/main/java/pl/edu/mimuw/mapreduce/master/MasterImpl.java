package pl.edu.mimuw.mapreduce.master;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.config.NetworkConfig;
import pl.edu.mimuw.proto.batchmanager.BatchManagerGrpc;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class MasterImpl extends MasterGrpc.MasterImplBase {
    private static final Logger logger = Logger.getLogger("pl.edu.mimuw.mapreduce.master");
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private FutureCallback<Response> createCallback(StreamObserver<Response> responseObserver) {
        return new FutureCallback<Response>() {
            @Override
            public void onSuccess(Response result) {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                Response response = Response.newBuilder().setStatusCode(StatusCode.Err).setMessage(t.getMessage()).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void submitBatch(Batch request, StreamObserver<Response> responseObserver) {
        String hostname;
        int port;

        if (NetworkConfig.IS_KUBERNETES) {
            hostname = NetworkConfig.BATCH_MANAGERS_HOST;
            port = NetworkConfig.BATCH_MANAGERS_PORT;
        } else {
            hostname = "localhost";
            port = 2137;
        }
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(hostname, port).executor(pool).usePlaintext().build();
        var batchManagerFutureStub = BatchManagerGrpc.newFutureStub(managedChannel);

        ListenableFuture<Response> listenableFuture = batchManagerFutureStub.doBatch(request);
        Futures.addCallback(listenableFuture, createCallback(responseObserver), pool);
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        throw new RuntimeException("todo");
    }
}
