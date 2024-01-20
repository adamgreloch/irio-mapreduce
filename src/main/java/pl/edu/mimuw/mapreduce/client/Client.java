package pl.edu.mimuw.mapreduce.client;

import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.common.ClusterConfig;
import pl.edu.mimuw.proto.common.Batch;
import pl.edu.mimuw.proto.common.Response;
import pl.edu.mimuw.proto.common.StatusCode;
import pl.edu.mimuw.proto.master.MasterGrpc;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class Client {
    public static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    private final MasterGrpc.MasterBlockingStub blockingStub;

    public Client(Channel channel) {
        this.blockingStub = MasterGrpc.newBlockingStub(channel);
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

    public static Optional<Batch> batchFromJson(String json) {
        var batchBuilder = Batch.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(json, batchBuilder);
            return Optional.of(batchBuilder.build());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            LOGGER.info("Expected 1 argument, but got: " + args.length);
            return;
        }
        String jsonFilePath = args[0];

        ManagedChannel channel = Utils.createCustomClientChannelBuilder(ClusterConfig.MASTERS_URI).build();

        try {
            Client client = new Client(channel);

            File jsonFile = new File(jsonFilePath);

            String json = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
            Optional<Batch> optionalBatch = batchFromJson(json);

            if (optionalBatch.isEmpty()) {
                LOGGER.info("Couldn't convert provided json to batch");
                return;
            }

            client.sendBatch(optionalBatch.get());
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

}
