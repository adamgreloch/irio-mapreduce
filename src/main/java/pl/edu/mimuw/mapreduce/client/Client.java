package pl.edu.mimuw.mapreduce.client;

import com.google.api.client.json.Json;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class Client {
    public static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    private final MasterGrpc.MasterBlockingStub blockingStub;

    public Client(Channel channel) {
        blockingStub = MasterGrpc.newBlockingStub(channel);
    }

    public void sendBatch(Batch batch) {
        LOGGER.info("Will try to send batch.");
        Response response;
        try {
            response = blockingStub.submitBatch(batch);
        } catch (Exception e) {
            LOGGER.info("RPC failed during processing batch", e);
            return;
        }
        if (response.getStatusCode() == StatusCode.Ok) {
            LOGGER.info("Finished processing batch. Output files are in: " + batch.getFinalDestDirId() + " directory.");
        } else {
            LOGGER.info("During batch processing error occurred:" + response.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1){
            LOGGER.info("Expected 1 argument, but got: " + args.length);
            return;
        }
        String json = args[0];

        Optional<Batch> optionalBatch = Utils.batchFromJson(json);

        if (optionalBatch.isEmpty()) {
            LOGGER.info("Couldn't convert provided json to batch");
            return;
        }

        ManagedChannel channel = Utils.createCustomClientChannelBuilder(ClusterConfig.TASK_MANAGERS_URI).build(); // IDK maybe add executor

        try {
            Client client = new Client(channel);
            client.sendBatch(optionalBatch.get());
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

}
