package pl.edu.mimuw.mapreduce.common;

import pl.edu.mimuw.proto.healthcheck.PingResponse;

public interface HealthCheckable {

    /**
     * Performs a healthcheck on itself and adjacent layers via gRPC if applicable.
     * This call should be handled in the same way as the gRPC healthCheck request.
     * Prints the result to the log.
     *
     * @return true if service and all lower layers are considered healthy, false otherwise
     */
   PingResponse internalHealthcheck();
}
