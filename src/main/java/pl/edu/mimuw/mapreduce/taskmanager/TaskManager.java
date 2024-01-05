package pl.edu.mimuw.mapreduce.taskmanager;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.common.Task;
import pl.edu.mimuw.proto.healthcheck.Ping;
import pl.edu.mimuw.proto.healthcheck.PingResponse;
import pl.edu.mimuw.proto.taskmanager.TaskManagerGrpc;

import java.io.IOException;

public class TaskManager {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new TaskManagerImpl(), 2137);
    }

    static class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase {
        @Override
        public void doTask(Task request, StreamObserver<Response> responseObserver) {
            Response response = Response.newBuilder().setStatusCode(StatusCode.TEST).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
