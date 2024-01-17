package pl.edu.mimuw.mapreduce.common;

public interface HealthCheckable {

    /**
     * Performs a healthcheck on itself and adjacent layers via gRPC if applicable.
     * This call should be handled in the same way as the gRPC healthCheck request.
     * Prints the result to the log.
     */
    void internalHealthcheck();
}
