package pl.edu.mimuw.mapreduce.serverbreaker;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.processbreaker.Payload;
import pl.edu.mimuw.proto.processbreaker.ServerBreakerGrpc;

public class ServerBreakerImpl extends ServerBreakerGrpc.ServerBreakerImplBase {
    @Override
    public void executePayload(Payload request, StreamObserver<Response> responseObserver) {

    }
}
