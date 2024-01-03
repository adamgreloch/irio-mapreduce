package pl.edu.mimuw.mapreduce.taskmanager;

import io.grpc.stub.StreamObserver;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.Task;
import pl.edu.mimuw.mapreduce.healthcheck.Ping;
import pl.edu.mimuw.mapreduce.healthcheck.PingResponse;

import java.io.IOException;

public class TaskManager {
    public static void main(String[] args) throws IOException, InterruptedException {
        Utils.start_service(new TaskManagerImpl(), 50044);
    }

    static class TaskManagerImpl extends TaskManagerGrpc.TaskManagerImplBase {
        @Override
        public void doTask(Task request, StreamObserver<TaskManagerDone> responseObserver) {
            throw new RuntimeException("todo");
        }

        @Override
        public void healthCheck(Ping request, StreamObserver<PingResponse> responseObserver) {
            throw new RuntimeException("todo");
        }
    }
}
