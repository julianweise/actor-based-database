package de.hpi.julianweise.benchmarking;

import lombok.AllArgsConstructor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ADBQueryPerformanceSampler {

    @AllArgsConstructor
    public static class LogDate {
        private final long relativeTime;
        private final long threadId;
        private final boolean isStart;
        private final String className;
        private final String purpose;
    }

    public static final boolean ENABLED = false;
    private static final String RESULT_DIR = System.getProperty("user.dir") + "/" + "query_performance_sampler_results";
    private static final Queue<LogDate> RESULT_COLLECTION = new ConcurrentLinkedQueue<>();

    public static String ensureResultDirExists(int transactionId) {
        String path = RESULT_DIR + "/TX#" + transactionId + "_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
        File directory = new File(path);
        if (!directory.exists()){
            boolean result = directory.mkdirs();
            if (!result) {
                System.out.println("Unable to create directory for ADBQueryPerformanceSampler results");
            }
        }
        return path;
    }

    public static void log(boolean isStart, String className, String purpose) {
        if (!ENABLED) {
            return;
        }
        RESULT_COLLECTION.add(
                new LogDate(System.currentTimeMillis(), Thread.currentThread().getId(), isStart, className, purpose));
    }

    public static void concludeSampler(int nodeId, int transactionId) {
        if (!ENABLED) {
            return;
        }
        String path = ADBQueryPerformanceSampler.ensureResultDirExists(transactionId);
        try {
            String fileName = path + "/Node#" + nodeId + ".csv";
            FileWriter csvWriter = new FileWriter(fileName);
            csvWriter.append("relative_time");
            csvWriter.append(",");
            csvWriter.append("thread_id");
            csvWriter.append(",");
            csvWriter.append("event");
            csvWriter.append(",");
            csvWriter.append("class_name");
            csvWriter.append(",");
            csvWriter.append("purpose");
            csvWriter.append("\n");

            for (LogDate row : RESULT_COLLECTION) {
                csvWriter.append(Long.toString(row.relativeTime));
                csvWriter.append(",");
                csvWriter.append(Long.toString(row.threadId));
                csvWriter.append(",");
                csvWriter.append(row.isStart ? "START" : "END");
                csvWriter.append(",");
                csvWriter.append(row.className);
                csvWriter.append(",");
                csvWriter.append(row.purpose);
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
