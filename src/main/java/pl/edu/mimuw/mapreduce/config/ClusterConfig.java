package pl.edu.mimuw.mapreduce.config;

import java.io.IOException;
import java.nio.file.Files;

public class ClusterConfig {
    public static final boolean IS_KUBERNETES = false;

    public static final String WORKERS_HOST = env_or("WORKERS_SERVICE_HOST", "localhost");
    public static final int WORKERS_PORT = env_or("WORKERS_SERVICE_PORT", 5045);

    public static final String TASK_MANAGERS_HOST = env_or("TASKMGR_SERVICE_HOST", "localhost");
    public static final int TASK_MANAGERS_PORT = env_or("TASKMGR_SERVICE_PORT", 5044);

    public static final String BATCH_MANAGERS_HOST = env_or("BATCHMGR_SERVICE_HOST", "localhost");
    public static final int BATCH_MANAGERS_PORT = env_or("BATCHMGR_SERVICE_PORT", 5043);

    public static final String MASTERS_HOST = env_or("MASTER_SERVICE_HOST", "localhost");
    public static final int MASTERS_PORT = env_or("MASTER_SERVICE_PORT", 5042);

    public static final String STORAGE_DIR;

    static {
        try {
            STORAGE_DIR = env_or("STORAGE_DIR", Files.createTempDirectory("storage").toAbsolutePath().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String env_or(String env, String str) {
        return IS_KUBERNETES ? System.getenv(env) : str;
    }

    static int env_or(String env, int port) {
        return IS_KUBERNETES ? Integer.parseInt(System.getenv(env)) : port;
    }
}
