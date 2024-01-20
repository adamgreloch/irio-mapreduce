package pl.edu.mimuw.mapreduce.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.processbreaker.Action;
import pl.edu.mimuw.proto.processbreaker.Payload;
import pl.edu.mimuw.proto.processbreaker.ServerBreakerGrpc;

import java.util.concurrent.TimeUnit;

public class ServerBreakerClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    private final ServerBreakerGrpc.ServerBreakerBlockingStub blockingStub;

    public ServerBreakerClient(Channel channel) {
        this.blockingStub = ServerBreakerGrpc.newBlockingStub(channel);
    }

    public void send(Action action, Long param) {
        Response response;

        Payload payload = Payload.newBuilder().setAction(action).setParam(param).build();
        try {
            response = blockingStub.executePayload(payload);
        } catch (Exception e) {
            LOGGER.info("RPC failed during processing payload", e);
            return;
        }
        if (response.getStatusCode() == StatusCode.Ok) {
            LOGGER.info("Finished processing payload.");
        } else {
            LOGGER.info("During payload processing error occurred:" + response.getMessage());
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            LOGGER.info("Expected 2 argument, but got: " + args.length);
            LOGGER.info("Arguments are (hostname), (Action) (param)");
            return;
        }
        String hostname = args[0];
        Action action;
        Long param = null;
        try {
            action = Action.valueOf(args[1]);
            if (action == Action.TIMEOUT){
                if(args.length != 3){
                    LOGGER.info("Expected third argument (param)");
                    return;
                }
                param = Long.valueOf(args[2]);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.info("Action must be one of: NONE, KILL, HANG, TIMEOUT, FAIL_ALWAYS");
            return;
        }
        ManagedChannel channel = Utils.createCustomClientChannelBuilder(hostname).build();
        try{
            ServerBreakerClient serverBreakerClient = new ServerBreakerClient(channel);

            serverBreakerClient.send(action, param);
        }finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
