package pl.edu.mimuw.mapreduce.serverbreaker;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.processbreaker.Action;
import pl.edu.mimuw.proto.processbreaker.Payload;
import pl.edu.mimuw.proto.processbreaker.ServerBreakerGrpc;

public final class ServerBreakerImpl extends ServerBreakerGrpc.ServerBreakerImplBase {
    private static volatile ServerBreakerImpl instance;
    private Action action;

    private ServerBreakerImpl() {
        this.action = Action.NONE;
    }

    @Override
    public void executePayload(Payload request, StreamObserver<Response> responseObserver) {
        this.action = request.getAction();
        Utils.respondWithSuccess(responseObserver);
    }

    public static ServerBreakerImpl getInstance() {
        // https://refactoring.guru/java-dcl-issue
        ServerBreakerImpl result = instance;
        if (result != null) {
            return result;
        }
        synchronized(ServerBreakerImpl.class) {
            if (instance == null) {
                instance = new ServerBreakerImpl();
            }
            return instance;
        }
    }
}
