package de.hpi.julianweise.benchmarking;

import lombok.AllArgsConstructor;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ADBQueryPerformanceSampler {

    @AllArgsConstructor
    public static class LogDate {
        private final long relativeTime;
        private final long threadId;
        private final String className;
        private final String information;
    }

    public static boolean ENABLED = true;
    private static final Queue<LogDate> RESULT_COLLECTION = new ConcurrentLinkedQueue<>();

    public void log(String className, String information) {
        if (!ENABLED) {
            return;
        }
        RESULT_COLLECTION.add(new LogDate(System.nanoTime(), Thread.currentThread().getId(), className, information));
    }

    public void concludeSampler(int transactionId) {
        if (!ENABLED) {
            return;
        }
        try {
            FileWriter csvWriter = new FileWriter("Query_Profile_TX#" + transactionId + "_" + LocalDateTime.now());
            csvWriter.append("relative_time");
            csvWriter.append(",");
            csvWriter.append("thread_id");
            csvWriter.append(",");
            csvWriter.append("class_name");
            csvWriter.append(",");
            csvWriter.append("information");
            csvWriter.append("\n");

            for (LogDate row : RESULT_COLLECTION) {
                csvWriter.append(Long.toString(row.relativeTime));
                csvWriter.append(Long.toString(row.threadId));
                csvWriter.append(row.className);
                csvWriter.append(row.information);
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
