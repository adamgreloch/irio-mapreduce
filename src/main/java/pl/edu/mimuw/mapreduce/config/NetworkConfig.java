package pl.edu.mimuw.mapreduce.config;

public class NetworkConfig {
    public static final boolean IS_KUBERNETES = false;

    public static final String WORKERS_HOST = System.getenv("WORKERS_SERVICE_HOST");
    public static final int WORKERS_PORT = Integer.parseInt(System.getenv("WORKERS_SERVICE_PORT"));
}
