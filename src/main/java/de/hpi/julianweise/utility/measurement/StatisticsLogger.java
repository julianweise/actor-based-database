package de.hpi.julianweise.utility.measurement;

import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class StatisticsLogger {

    private static StatisticsLogger INSTANCE;
    public static void setInstance(StatisticsLogger instance) {
        StatisticsLogger.INSTANCE = instance;
    }
    private final static Logger LOG = LoggerFactory.getLogger(StatisticsLogger.class);
    private static final String statsDir = "a2db_stats/";

    private final BufferedWriter bufferedWriter;

    public static StatisticsLogger getInstance() {
        return INSTANCE;
    }

    @SneakyThrows
    public StatisticsLogger(){
        this.bufferedWriter = Files.newBufferedWriter(this.getStatsFile().toPath());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public File getStatsFile() throws IOException {
        if (!this.ensureResultDirectoryExists()) {
            LOG.error("Unable to create result directory!");
            return new File(System.getProperty("userDir"));
        }
        String resultFileName = String.format("Stats_%s.csv", UUID.randomUUID().toString());
        Path filePath = Paths.get(StatisticsLogger.statsDir, resultFileName);
        File resultFile = filePath.toFile();
        LOG.info("Creating statistics file at {}", resultFile.getAbsolutePath());
        if (!resultFile.exists()){
            resultFile.getParentFile().mkdirs();
            if (!resultFile.createNewFile()) {
                LOG.error("Unable to create result file {}", resultFileName);
            }
        }
        return resultFile;
    }

    private boolean ensureResultDirectoryExists() {
        File resultDirectory = new File(StatisticsLogger.statsDir);
        if (resultDirectory.exists()) {
            return true;
        }
        return resultDirectory.mkdirs();
    }

    @SneakyThrows
    public void logPredicate(ADBJoinPredicateCostModel predicate) {
        this.bufferedWriter.write("predicate," + predicate.getRelativeCost());
    }

    @SneakyThrows
    public void logPredicates(ObjectList<ADBJoinPredicateCostModel> predicates, SettingsImpl settings) {
        int low = 0, mid = 0, high = 0;
        for (ADBJoinPredicateCostModel predicate : predicates) {
            if (predicate.getRelativeCost() <= settings.JOIN_STRATEGY_LOWER_BOUND) {
                low++;
            } else if (predicate.getRelativeCost() <= settings.JOIN_STRATEGY_UPPER_BOUND) {
                mid++;
            } else {
                high++;
            }
        }
        this.bufferedWriter.write("predicate-buckets," + low + "," + mid + "," + high);
        this.bufferedWriter.newLine();
    }

    @SneakyThrows
    public void logMinMaxFiltering(int originalSize, int transferred, String type) {
        this.bufferedWriter.write("min-max," + originalSize + "," + transferred + "," + type);
        this.bufferedWriter.newLine();
    }
}
