package pl.edu.mimuw.mapreduce.common;

import java.io.IOException;
import java.nio.file.Files;

public class ClusterConfig {
    public static final boolean IS_KUBERNETES = false;

    public static final String WORKERS_URI = env_or("WORKERS_SERVICE_URI", "localhost:5045");

    public static final String TASK_MANAGERS_URI = env_or("TASKMGR_SERVICE_URI", "localhost:5044");

    public static final String MASTERS_URI = env_or("MASTER_SERVICE_HOST", "localhost:5042");

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
}
