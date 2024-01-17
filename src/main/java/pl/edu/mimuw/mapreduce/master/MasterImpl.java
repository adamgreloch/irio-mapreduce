package pl.edu.mimuw.mapreduce.master;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.config.ClusterConfig;
import pl.edu.mimuw.proto.batchmanager.BatchManagerGrpc;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.healthcheck.MissingConnectionWithLayer;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterImpl extends MasterGrpc.MasterImplBase {
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ManagedChannel batchManagerChannel = Utils.createCustomManagedChannelBuilder(ClusterConfig.BATCH_MANAGERS_URI).executor(pool).usePlaintext().build();

    private FutureCallback<Response> createCallback(StreamObserver<Response> responseObserver) {
        return new FutureCallback<Response>() {
            @Override
            public void onSuccess(Response result) {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

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
    public void submitBatch(Batch request, StreamObserver<Response> responseObserver) {
        var batchManagerFutureStub = BatchManagerGrpc.newFutureStub(batchManagerChannel);

        ListenableFuture<Response> listenableFuture = batchManagerFutureStub.doBatch(request);
        Futures.addCallback(listenableFuture, createCallback(responseObserver), pool);
    }

    @Override
    public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
        var batchManagerFutureStub = BatchManagerGrpc.newFutureStub(batchManagerChannel);

        ListenableFuture<PingResponse> listenableFuture =
                batchManagerFutureStub.healthCheck(request);
        Futures.addCallback(listenableFuture, Utils.createHealthCheckResponse(responseObserver,
                MissingConnectionWithLayer.BatchManager), pool);
    }
}
